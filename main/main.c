#include <stdio.h>
#include <stdbool.h>
#include <unistd.h>

#include "esp_mesh.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/queue.h"

#include "esp_log.h"
#include "portmacro.h"
#include "inttypes.h" 

#define RAW_QUEUE_LENGTH 			5
#define PROCESSED_QUEUE_LENGTH 		5

#define PRODUSER_PERIOD_MS			1000

#define QUEUE_SEND_TIMEOUT_MS		100

#define PRODUSER_TASK_STACK_SIZE	3072
#define PROCESSIND_TASK_STACK_SIZE	3072
#define LOGGER_TASK_STACK_SIZE		3072

#define PRODUSER_TASK_PRIORITY		5
#define PROCESSIND_TASK_PRIORITY	5
#define LOGGER_TASK_PRIORITY		5

#define ADC_MAX_VALUE				4095U
#define ADC_REFFERENCE_VOLUME_mv	3300U			

#define MONITORING_PERION_MS 		5000
#define MONITOR_TASK_STACK_SIZE		3072
#define MONITOR_TASK_PRIORITY		4

#define PRODUSER_ACTIVITY_BIT		(1UL << 0)
#define PROCESSING_ACTIVITY_BIT		(1UL << 1)
#define LOGGER_ACTIVITY_BIT			(1UL << 2)

#define ALL_ACTIVITY_BITS 			(PRODUSER_ACTIVITY_BIT | PROCESSING_ACTIVITY_BIT | LOGGER_ACTIVITY_BIT)

typedef struct{
	uint32_t sequence_number;
	int32_t raw_adc_value;
	TickType_t timestamp;
}raw_sensor_message_t ;

typedef struct{
	uint32_t sequence_number;
	uint16_t raw_adc_value;
	uint32_t voltage_mv;
	
	TickType_t source_timestamp;
	TickType_t processed_timestemp;
}processed_sensor_message_t;

static const char *TAG = "TASK_COMMUNICATION";

static QueueHandle_t raw_data_queue = NULL;
static QueueHandle_t processed_data_queue = NULL;

static TaskHandle_t monitor_task_handle = NULL;



static void notify_monitor(uint32_t activity_bit)
{
	if(monitor_task_handle == NULL)
	{
		ESP_LOGW(TAG, "Monitor task handle is null");
		return;
	}
	
	BaseType_t status = xTaskNotify(monitor_task_handle, activity_bit, eSetBits);
	if(status != pdPASS)
	{
		ESP_LOGW(TAG, "Failed to monitor, bit=0x%" PRIX32, activity_bit);
	}
}


static void produser_task(void *parameters)
{
	uint32_t sequence_number = 0;
	int32_t simulited_adc_value = 1000;
	
	TickType_t last_wake_time = xTaskGetTickCount();
	ESP_LOGI(TAG, "produser_task started");
	 
	while(1)
	{
		raw_sensor_message_t raw_message =
		{
			.sequence_number = sequence_number,
			.raw_adc_value = simulited_adc_value,
			.timestamp = xTaskGetTickCount()
		};
		
		BaseType_t status = xQueueSend(raw_data_queue, &raw_message, pdMS_TO_TICKS(QUEUE_SEND_TIMEOUT_MS));
		if(status == pdPASS)
		{
			ESP_LOGI(TAG, 
			"Prodused sent: sequense=%" PRIu32
			", valude=%" PRIu32
			", timestemp=%" PRIu32,
			raw_message.sequence_number, 
			raw_message.raw_adc_value,
			(uint32_t)raw_message.timestamp);	
			notify_monitor(PRODUSER_ACTIVITY_BIT);
		}
		else 
		{
			ESP_LOGW(TAG, "Producer failed to send message: queue is full");	
		}
		
		sequence_number++;
		simulited_adc_value = simulited_adc_value + 250;
		if(simulited_adc_value > 4000)
		{
			simulited_adc_value = 1000;
		}		
		vTaskDelayUntil(&last_wake_time, pdMS_TO_TICKS(PRODUSER_PERIOD_MS));
	}	
}

static void processing_task(void *parameters)
{
	raw_sensor_message_t raw_message;
	
	ESP_LOGI(TAG, "processing_task started");
	
	while(1)
	{
		BaseType_t recived_status = xQueueReceive(raw_data_queue, &raw_message, portMAX_DELAY);
		if(recived_status == pdPASS)
		{
			processed_sensor_message_t processed_message = 
			{
				.sequence_number = raw_message.sequence_number,
				.raw_adc_value = raw_message.raw_adc_value,
				
				.voltage_mv = ((uint32_t)raw_message.raw_adc_value * ADC_REFFERENCE_VOLUME_mv)/ ADC_MAX_VALUE,
				
				.source_timestamp = raw_message.timestamp,
				.processed_timestemp = xTaskGetTickCount()
			};
			
			BaseType_t status = xQueueSend(processed_data_queue, &processed_message, pdMS_TO_TICKS(QUEUE_SEND_TIMEOUT_MS));
			if(status == pdPASS)
			{
				ESP_LOGI(TAG, 
					"Processing complete: sequense=%" PRIu32
					", voltage=%" PRIu32 "mV",
					processed_message.sequence_number, 
					processed_message.voltage_mv
					);
					
					notify_monitor(PROCESSING_ACTIVITY_BIT);
			}
			else 
			{
				ESP_LOGW(TAG, "Processing failed to send: processed queue is full");	
			}
		}
	}
}

static void logger_task(void *parameters)
{
	processed_sensor_message_t processed_message;
	
	ESP_LOGI(TAG, "logger_task started");
	
	while(1)
	{
		BaseType_t status = xQueueReceive(processed_data_queue, &processed_message, pdMS_TO_TICKS(QUEUE_SEND_TIMEOUT_MS));
		if(status == pdPASS)
		{
			TickType_t processing_delay = processed_message.processed_timestemp - processed_message.source_timestamp;
			
			ESP_LOGI(TAG, 
				"Logger received: sequense=%" PRIu32
				", raw_adc=%" PRIu16
				", voltage=%" PRIu32 "mV"
				", processing_delay=%" PRIu32 "ticks",
				processed_message.sequence_number,
				processed_message.raw_adc_value,
				processed_message.voltage_mv,
				(uint32_t)processing_delay
			);
			notify_monitor(LOGGER_ACTIVITY_BIT);
		}
	}
}

static void monitor_task(void *parameters)
{
	TickType_t last_wake_time = xTaskGetTickCount();
	
	ESP_LOGI(TAG, "monitor_task started");
	
	while(1)
	{
		vTaskDelayUntil(&last_wake_time, pdMS_TO_TICKS(MONITORING_PERION_MS));
		
		uint32_t activity_bits = 0;
		BaseType_t status = xTaskNotifyWait(0, UINT32_MAX, &activity_bits, 0);
		
		if(status != pdTRUE)
		{
			ESP_LOGW(TAG, "MONITOR: No task activity notifications received");
			continue;
		}
		ESP_LOGI(TAG, 
			"Monitor: bits:0x%" PRIX32
			", producer = %s"
			", processing = %s"
			", logger = %s",
			activity_bits,
			(activity_bits & PRODUSER_ACTIVITY_BIT)
				? "ACTIVE"
				: "MISSING",
			(activity_bits & PROCESSING_ACTIVITY_BIT)
				? "ACTIVE"
				: "MISSING",
			(activity_bits & LOGGER_ACTIVITY_BIT)
				? "ACTIVE"
				: "MISSING"
		);
		if((activity_bits & ALL_ACTIVITY_BITS) == ALL_ACTIVITY_BITS)
		{
			ESP_LOGI(TAG, "MONITOR: All task are active");
		}
		else 
		{
			ESP_LOGI(TAG, "MONITOR: one or more tasks did not report activity");
		}
		
	
	}
}

void app_main(void)
{
	ESP_LOGI(TAG, "Aplication started");
	
	raw_data_queue = xQueueCreate(RAW_QUEUE_LENGTH, sizeof(raw_sensor_message_t));
	if(raw_data_queue == NULL)
	{
		ESP_LOGE(TAG, "Falied to create raw_data_queue");
		return;
	}
	
	processed_data_queue = xQueueCreate(PROCESSED_QUEUE_LENGTH, sizeof(processed_sensor_message_t));
	if(processed_data_queue == NULL)
	{
		ESP_LOGE(TAG, "Falied to create processed_data_queue");
		return;
	}
	
	BaseType_t monitor_status = xTaskCreate(monitor_task, "monitor_task", MONITOR_TASK_STACK_SIZE, NULL, MONITOR_TASK_PRIORITY, &monitor_task_handle);
	if(monitor_status != pdPASS)
	{
		ESP_LOGE(TAG,"Failed create monitor_status");
		return;	
	}
	
	BaseType_t producer_status = xTaskCreate(produser_task, "produser_task", PRODUSER_TASK_STACK_SIZE, NULL, PRODUSER_TASK_PRIORITY, NULL);
	if(producer_status != pdPASS)
	{
		ESP_LOGE(TAG,"Failed create produser_task");
		return;	
	}
	
	BaseType_t processing_status = xTaskCreate(processing_task, "processing_task", PRODUSER_TASK_STACK_SIZE, NULL, PRODUSER_TASK_PRIORITY, NULL);
	if(processing_status != pdPASS)
	{
		ESP_LOGE(TAG,"Failed create processing_status");
		return;	
	}
	
	BaseType_t logger_status = xTaskCreate(logger_task, "logger_task", PRODUSER_TASK_STACK_SIZE, NULL, PRODUSER_TASK_PRIORITY, NULL);
	if(logger_status != pdPASS)
	{
		ESP_LOGE(TAG,"Failed create logger_task");
		return;	
	}
	

	
	
	
	ESP_LOGI(TAG, "Tasks and queue create susesfuly");	
}



























