#include <stdio.h>
#include <stdbool.h>
#include <unistd.h>

#include "esp_mesh.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/queue.h"

#include "esp_log.h"

#define SENSOR_QUEUE_LENGTH 	5
#define PRODUSER_PERIOD_MS		1000
#define QUEUE_SEND_TIMEOUT_MS	100

#define PRODUSER_TASK_STACK_SIZE	3072
#define CONSUMER_TASK_STACK_SIZE	3072

#define PRODUSER_TASK_PRIORITY	5
#define CONSUMER_TASK_PRIORITY	5

typedef struct{
	uint32_t sequence_number;
	int32_t value;
	TickType_t timestamp;
}sensor_message_t;

static const char *TAG = "TASK_COMMUNICATION";

static QueueHandle_t sensor_data_queue = NULL;


static void produser_task(void *parameters)
{
	uint32_t sequence_number = 0;
	int32_t simulited_value = 20;
	
	TickType_t last_wake_time = last_wake_time = xTaskGetTickCount();
	ESP_LOGI(TAG, "produser_task started");
	 
	while(1)
	{
		sensor_message_t message =
		{
			.sequence_number = sequence_number,
			.value = simulited_value,
			.timestamp = xTaskGetTickCount()
		};
		
		BaseType_t status = xQueueSend(sensor_data_queue, &message, pdMS_TO_TICKS(QUEUE_SEND_TIMEOUT_MS));
		if(status == pdPASS)
		{
			ESP_LOGI(TAG, 
			"Prodused sent: sequense=%" PRIu32
			", valude=%" PRIu32
			", timestemp=%" PRIu32,
			message.sequence_number, 
			message.value,
			(uint32_t)message.timestamp);	
		}
		else 
		{
			ESP_LOGW(TAG, "Producer failed to send message: queue is full");	
		}
		
		sequence_number++;
		simulited_value++;
		if(simulited_value > 30)
		{
			simulited_value = 20;
		}		
		vTaskDelayUntil(&last_wake_time, pdMS_TO_TICKS(PRODUSER_PERIOD_MS));
	}	
}

static void consumer_task(void *parameters)
{
	sensor_message_t received_message;
	
	ESP_LOGI(TAG, "consumer_task started");
	
	while(1)
	{
		BaseType_t status = xQueueReceive(sensor_data_queue, &received_message, portMAX_DELAY);
		if(status == pdPASS)
		{
			ESP_LOGI(TAG, 
			"Prodused reseived: sequense=%" PRIu32
			", valude=%" PRIu32
			", timestemp=%" PRIu32,
			received_message.sequence_number, 
			received_message.value,
			(uint32_t)received_message.timestamp);	
		}
	}
}


void app_main(void)
{
	ESP_LOGI(TAG, "Aplication started");
	
	sensor_data_queue = xQueueCreate(SENSOR_QUEUE_LENGTH, sizeof(sensor_message_t));
	if(sensor_data_queue == NULL)
	{
		ESP_LOGE(TAG, "Falied to create queue");
		return;
	}
	
	BaseType_t producer_status = xTaskCreate(produser_task, "produser_task", PRODUSER_TASK_STACK_SIZE, NULL, PRODUSER_TASK_PRIORITY, NULL);
	if(producer_status != pdPASS)
	{
		ESP_LOGE(TAG,"Failed create produser_task");
		vQueueDelete(sensor_data_queue);
		sensor_data_queue = NULL;
		return;	
	}
	
	producer_status = xTaskCreate(consumer_task, "consumer_task", PRODUSER_TASK_STACK_SIZE, NULL, PRODUSER_TASK_PRIORITY, NULL);
	if(producer_status != pdPASS)
	{
		ESP_LOGE(TAG,"Failed create consumer_task");
		return;	
	}
	
	ESP_LOGI(TAG, "Tasks and queue create susesfuly");
	
}



























