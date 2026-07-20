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

#include "esp_system.h"
#include "string.h"
#include "sdkconfig.h"
#include "esp_cpu.h"

#define RAW_QUEUE_LENGTH 			5
#define PROCESSED_QUEUE_LENGTH 		1

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

#define NOTIFICATION_COUNTER_PERIOD			5000
#define NOTIFICATION_COUNTER_STASK_SIZE		2028
#define NOTIFICATION_COUNTER_PRIORITY		4

#define LOGGER_WORK_DELAY_MS		3000

#define CPU_STATS_PERIOD_MS			10000
#define CPU_STATS_TASK_STACK_SIZE	4096
#define CPU_STATS_TASK_PRIORITY		2
#define CPU_STATS_BUFFER_SIZE		4096

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


static TaskHandle_t produser_task_handle = NULL;
static TaskHandle_t processing_status_task_handle = NULL;
static TaskHandle_t logger_status_task_handle = NULL;
static TaskHandle_t monitor_task_handle = NULL;
static TaskHandle_t notification_counter_task_handle = NULL;
static TaskHandle_t cpu_status_task_handle = NULL;

char cpu_status_buffer[CPU_STATS_BUFFER_SIZE];


static void log_task_stack(const char *task_name, TaskHandle_t task_handle)
{
	if(task_handle == NULL)
	{
		ESP_LOGW("RESURCE_MONITOR", "%s handle is NULL", task_name);
		return;
	}
	UBaseType_t minimum_free_size = uxTaskGetStackHighWaterMark(task_handle);
	ESP_LOGI("RESURCE_MONITOR", "%s minimum free stack: %u bytes", task_name, (unsigned int)minimum_free_size);
	
	ESP_LOGI(TAG, "WORK ON CORE %d",
		esp_cpu_get_core_id());
}
static void log_system_resources(void)
{
	//uint32_t free_heap = esp_get_free_heap_size();
	size_t total_haep = heap_caps_get_total_size(MALLOC_CAP_DEFAULT);
	size_t free_heap = heap_caps_get_free_size(MALLOC_CAP_DEFAULT);
	size_t minimum_free_haep = esp_get_minimum_free_heap_size();
	size_t used_heap = total_haep - free_heap;
	
	UBaseType_t raw_message = uxQueueMessagesWaiting(raw_data_queue);
	UBaseType_t raw_spases = uxQueueSpacesAvailable(raw_data_queue);
	UBaseType_t processed_message = uxQueueMessagesWaiting(processed_data_queue);
	UBaseType_t processed_spases = uxQueueSpacesAvailable(processed_data_queue);
	
	ESP_LOGI("RESURCE_MONITOR",
		" Heap: Total Heap =%u, Free Heap: =%u bytes, Used Heap =%u, minimum free heap =%u",
		(unsigned int)total_haep,
		(unsigned int)free_heap,
		(unsigned int)used_heap,
		(unsigned int)minimum_free_haep);
	
	
	ESP_LOGI("RESURCE_MONITOR",
		"Raw queue: messages=%u, free=%u, capasity=%u",
		(unsigned int)raw_message,
		(unsigned int)raw_spases,
		(unsigned int)RAW_QUEUE_LENGTH);
	
		ESP_LOGI("RESURCE_MONITOR",
		"Processing queue: messages=%u, free=%u, capasity=%u",
		(unsigned int)processed_message,
		(unsigned int)processed_spases,
		(unsigned int)PROCESSED_QUEUE_LENGTH);
	
		log_task_stack("produser_task", produser_task_handle);
		log_task_stack("processing_status_task", processing_status_task_handle);
		log_task_stack("logger_status_task", logger_status_task_handle);
		log_task_stack("produser_task", produser_task_handle);
		log_task_stack("monitor_task", monitor_task_handle);
		log_task_stack("notification_counter_task", notification_counter_task_handle);
		log_task_stack("cpu_status_task", cpu_status_task_handle);
		
		ESP_LOGI(TAG, "WORK ON CORE %d",
		esp_cpu_get_core_id());
}
static void cpu_status_task(void *parametrs)
{
	static const char *TAG = "CPU_STATUS";
	TickType_t last_weak_time = xTaskGetTickCount();
	ESP_LOGI(TAG, "cpu_status_task started");
	
	while(1)
	{
		vTaskDelayUntil(&last_weak_time, pdMS_TO_TICKS(CPU_STATS_PERIOD_MS));
		memset(cpu_status_buffer, 0, sizeof(cpu_status_buffer));
		
#if CONFIG_FREERTOS_GENERATE_RUN_TIME_STATS && \
    CONFIG_FREERTOS_USE_STATS_FORMATTING_FUNCTIONS && \
    CONFIG_FREERTOS_USE_TRACE_FACILITY
    
    vTaskGetRunTimeStats(cpu_status_buffer);
    ESP_LOGI(TAG, 
    	"\n"
    	"Task runtime statistics:\n"
    	"Task name\tRuntime\tCPU %%\n"
    	"%s",
    	cpu_status_buffer
    );
#else
    ESP_LOGE(TAG, "FreeRTOS runtime statistics are disabled")
#endif	
	}
	
	ESP_LOGI(TAG, "WORK ON CORE %d",
	esp_cpu_get_core_id());
}

static void notify_counter_task(void) 
{
	static const char *TAG = "NOTIFY COUNTER TASK";
	
	if(notification_counter_task_handle == NULL)
	{
		ESP_LOGW(TAG, "notification counter task handle is NULL");
		return;
	}
	BaseType_t status = xTaskNotifyGive(notification_counter_task_handle);
	if(status != pdPASS)
	{
		ESP_LOGW(TAG, "Failed increment notification counter");
	}
}

static void notify_monitor(uint32_t activity_bit)
{
	static const char *TAG = "NITIFY MONITOR";
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
	ESP_LOGI(TAG, "WORK ON CORE %d",
	esp_cpu_get_core_id());
}

static void produser_task(void *parameters)
{
	static const char *TAG = "PRODUSER_TASK";
	
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
			UBaseType_t raw_messages_waiting = uxQueueMessagesWaiting(raw_data_queue);
			
			ESP_LOGI(TAG, 
			"Prodused sent: sequense=%" PRIu32
			", valude=%" PRIu32
			", timestemp=%" PRIu32
			", aw_queu=%u/%u",
			raw_message.sequence_number, 
			raw_message.raw_adc_value,
			(uint32_t)raw_message.timestamp,
			(unsigned int)raw_messages_waiting,
			(unsigned int)RAW_QUEUE_LENGTH);	
			
			notify_monitor(PRODUSER_ACTIVITY_BIT);
			
			notify_counter_task();
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
		
		ESP_LOGI(TAG, "WORK ON CORE %d",
		esp_cpu_get_core_id());	
		
		vTaskDelayUntil(&last_wake_time, pdMS_TO_TICKS(PRODUSER_PERIOD_MS));
	}	
}

static void processing_task(void *parameters)
{
	static const char *TAG = "PROCESSING TASK";
	
	raw_sensor_message_t raw_message;
	
	
	ESP_LOGI(TAG, "processing_task started");
	
	while(1)
	{
		BaseType_t recived_status = xQueueReceive(raw_data_queue, &raw_message, portMAX_DELAY);
		if(recived_status != pdPASS)
		{
			continue;
		}
		
		processed_sensor_message_t processed_message = 
		{
			.sequence_number = raw_message.sequence_number,
			.raw_adc_value = raw_message.raw_adc_value,
				
			.voltage_mv = ((uint32_t)raw_message.raw_adc_value * ADC_REFFERENCE_VOLUME_mv)/ ADC_MAX_VALUE,
				
			.source_timestamp = raw_message.timestamp,
			.processed_timestemp = xTaskGetTickCount()
		};	
		
		BaseType_t owerwrite_status = xQueueOverwrite(processed_data_queue, &processed_message);
		
		if(owerwrite_status == pdPASS)
		{
			UBaseType_t messages_writing = uxQueueMessagesWaiting(processed_data_queue);
			
			ESP_LOGI(TAG, 
			"Processing sent: sequense=%" PRIu32
			", voltage=%" PRIu32 "mV"
			", queue=%u/%u",
			processed_message.sequence_number, 
			processed_message.voltage_mv,
			(unsigned int)messages_writing,
			(unsigned int)PROCESSED_QUEUE_LENGTH);
			
			notify_monitor(PROCESSING_ACTIVITY_BIT);
		} 
		else 
		{
			ESP_LOGE(TAG, "Failet to write processed sequense=%" PRIu32,
				processed_message.sequence_number);
		}
		ESP_LOGI(TAG, "WORK ON CORE %d",
		esp_cpu_get_core_id());
	}
}

static void logger_task(void *parameters)
{
	static const char *TAG = "LOGGER TASK";
	
	processed_sensor_message_t processed_message;
	
	uint32_t expexted_sequence_number = 0;
	bool sequence_initialized = false;
	
	ESP_LOGI(TAG, "logger_task started");
	
	while(1)
	{
		BaseType_t status = xQueueReceive(processed_data_queue, &processed_message, portMAX_DELAY);
		if(status != pdPASS)
		{
			continue;
		}
		if(sequence_initialized == false)
		{
			expexted_sequence_number = processed_message.sequence_number;
			sequence_initialized = true;
		}
		
		if(processed_message.sequence_number != expexted_sequence_number)
		{
			if(processed_message.sequence_number > expexted_sequence_number)
			{
				uint32_t lost_messages = processed_message.sequence_number - expexted_sequence_number;
				
				ESP_LOGI(TAG,
					"Logger detected seqence gup:"
					" expected=%" PRIu32
					" received=%" PRIu32
					" lost=%" PRIu32,
					expexted_sequence_number,
					processed_message.sequence_number,
					lost_messages);
			}
			else 
			{
				ESP_LOGW(TAG,
					"Logger detected old or out-of-order message:"
					" expected=%" PRIu32
					" received=%" PRIu32,
					expexted_sequence_number,
					processed_message.sequence_number);
			}
		}
		expexted_sequence_number = processed_message.sequence_number + 1;
		
		TickType_t processing_delay = processed_message.processed_timestemp - processed_message.source_timestamp;
		
		ESP_LOGI(TAG,
			"Logger received: sequence=%" PRIu32
			", raw_adc=%" PRIu16
			", voltage=%" PRIu32 "mV"
			", processing delay=%" PRIu32 " ricks",
			processed_message.sequence_number,
			processed_message.raw_adc_value,
			processed_message.voltage_mv,
			(uint32_t)processing_delay
		);
		notify_monitor(LOGGER_ACTIVITY_BIT);
		
		ESP_LOGI(TAG, "WORK ON CORE %d",
		esp_cpu_get_core_id());
		
		vTaskDelay(pdMS_TO_TICKS(LOGGER_WORK_DELAY_MS));
	}
}

static void monitor_task(void *parameters)
{
	TickType_t last_wake_time = xTaskGetTickCount();
	
	static const char *TAG = "MONITOR TASK";
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
		log_system_resources();
		
		ESP_LOGI(TAG, "WORK ON CORE %d",
		esp_cpu_get_core_id());
	}
}
static void nitification_counter_task(void *parameters)
{
	static const char *TAG = "NOTIFICATION COUNTER TASK";
	
	TickType_t last_wake_time = xTaskGetTickCount();
	
	ESP_LOGI(TAG, "nitification_counter_task started");
	
	while(1)
	{
		vTaskDelayUntil(&last_wake_time, pdMS_TO_TICKS(NOTIFICATION_COUNTER_PERIOD));
		uint32_t produser_ivent = ulTaskNotifyTake(pdTRUE, 0);
		ESP_LOGI(TAG, "Producer events during last period: %" PRIu32, produser_ivent);
			
		ESP_LOGI(TAG, "WORK ON CORE %d",
		esp_cpu_get_core_id());
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
	
	BaseType_t cpu_status_status = xTaskCreate(cpu_status_task, "cpu_status_task", MONITOR_TASK_STACK_SIZE, NULL, MONITOR_TASK_PRIORITY, &cpu_status_task_handle);
	if(cpu_status_status != pdPASS)
	{
		ESP_LOGE(TAG,"Failed create cpu_status_task_handle");
		return;	
	}
	
	BaseType_t monitor_status = xTaskCreate(monitor_task, "monitor_task", MONITOR_TASK_STACK_SIZE, NULL, MONITOR_TASK_PRIORITY, &monitor_task_handle);
	if(monitor_status != pdPASS)
	{
		ESP_LOGE(TAG,"Failed create monitor_status");
		return;	
	}
	
	BaseType_t notification_counter_task_handle_status = xTaskCreate(nitification_counter_task, "nitification_counter_task", NOTIFICATION_COUNTER_STASK_SIZE, NULL, NOTIFICATION_COUNTER_PRIORITY, &notification_counter_task_handle);
	if(notification_counter_task_handle_status != pdPASS)
	{
		ESP_LOGE(TAG,"Failed create nitification_counter_task");
		return;	
	}
	
	BaseType_t producer_status = xTaskCreate(produser_task, "produser_task", PRODUSER_TASK_STACK_SIZE, NULL, PRODUSER_TASK_PRIORITY, &produser_task_handle);
	if(producer_status != pdPASS)
	{
		ESP_LOGE(TAG,"Failed create produser_task");
		return;	
	}
	
	BaseType_t processing_status = xTaskCreate(processing_task, "processing_task", PROCESSIND_TASK_STACK_SIZE, NULL, PRODUSER_TASK_PRIORITY, &processing_status_task_handle);
	if(processing_status != pdPASS)
	{
		ESP_LOGE(TAG,"Failed create processing_status");
		return;	
	}
	
	BaseType_t logger_status = xTaskCreate(logger_task, "logger_task", LOGGER_TASK_STACK_SIZE, NULL, PRODUSER_TASK_PRIORITY, &logger_status_task_handle);
	if(logger_status != pdPASS)
	{
		ESP_LOGE(TAG,"Failed create logger_task");
		return;	
	}
	

	
	
	
	ESP_LOGI(TAG, "Tasks and queue create susesfuly");	
}



























