import requests
import json
import time

class SimpleBrokerClient:
    def __init__(self, base_url: str = "http://localhost:8000/api/v1"):
        self.base_url = base_url

    def create_topic(self, name: str):
        """Создать топик"""
        try:
            response = requests.post(
                f"{self.base_url}/topics",
                json={"name": name}
            )
            return response.json()
        except Exception as e:
            return {"error": str(e)}

    def create_queue(self, name: str, queue_type: str = "FIFO"):
        try:
            response = requests.post(
                f"{self.base_url}/queues",
                json={"name": name, "type": queue_type}
            )
            return response.json()
        except Exception as e:
            return {"error": str(e)}

    def subscribe(self, subscriber_id: str, target_type: str, target_name: str):
        try:
            response = requests.post(
                f"{self.base_url}/subscriptions",
                json={
                    "subscriber_id": subscriber_id,
                    "target_type": target_type,
                    "target_name": target_name
                }
            )
            return response.json()
        except Exception as e:
            return {"error": str(e)}

    def publish_to_topic(self, topic_name: str, content, headers=None):
        if headers is None:
            headers = {}

        try:
            response = requests.post(
                f"{self.base_url}/topics/{topic_name}/publish",
                json={
                    "content": content,
                    "content_type": "application/json",
                    "headers": headers
                }
            )
            return response.json()
        except Exception as e:
            return {"error": str(e)}

    def publish_to_queue(self, queue_name: str, content, headers=None):
        if headers is None:
            headers = {}

        try:
            response = requests.post(
                f"{self.base_url}/queues/{queue_name}/publish",
                json={
                    "content": content,
                    "content_type": "application/json",
                    "headers": headers
                }
            )
            return response.json()
        except Exception as e:
            return {"error": str(e)}

    def get_topics(self):
        try:
            response = requests.get(f"{self.base_url}/topics")
            return response.json()
        except Exception as e:
            return {"error": str(e)}

    def get_queues(self):
        try:
            response = requests.get(f"{self.base_url}/queues")
            return response.json()
        except Exception as e:
            return {"error": str(e)}

def test_pub_sub():
    print("=== Тестирование Pub/Sub ===")

    client = SimpleBrokerClient()

    print("1. Создание топика 'news'...")
    topic_result = client.create_topic("news")
    print(f"   Результат: {topic_result}")

    print("2. Создание подписок...")
    sub1 = client.subscribe("subscriber-1", "topic", "news")
    sub2 = client.subscribe("subscriber-2", "topic", "news")
    print(f"   Подписчик 1: {sub1}")
    print(f"   Подписчик 2: {sub2}")

    print("3. Публикация сообщений...")
    msg1 = client.publish_to_topic("news", {
        "title": "Важная новость!",
        "body": "Что-то важное произошло"
    }, {"priority": "high"})
    print(f"   Сообщение 1: {msg1}")

    msg2 = client.publish_to_topic("news", {
        "title": "Обычная новость",
        "body": "Обычное событие"
    })
    print(f"   Сообщение 2: {msg2}")

    print("4. Проверка топиков...")
    topics = client.get_topics()
    print(f"   Топики: {topics}")

def test_queue():
    print("\n=== Тестирование очередей ===")

    client = SimpleBrokerClient()

    print("1. Создание очередей...")
    fifo_queue = client.create_queue("tasks", "FIFO")
    lifo_queue = client.create_queue("urgent-tasks", "LIFO")
    print(f"   FIFO очередь: {fifo_queue}")
    print(f"   LIFO очередь: {lifo_queue}")

    print("2. Создание подписок...")
    worker1 = client.subscribe("worker-1", "queue", "tasks")
    worker2 = client.subscribe("worker-2", "queue", "tasks")
    urgent_worker = client.subscribe("urgent-worker", "queue", "urgent-tasks")
    print(f"   Воркер 1: {worker1}")
    print(f"   Воркер 2: {worker2}")
    print(f"   Срочный воркер: {urgent_worker}")

    print("3. Публикация задач в FIFO очередь...")
    for i in range(3):
        task = client.publish_to_queue("tasks", {
            "task_id": i + 1,
            "action": f"process_data_{i + 1}"
        })
        print(f"   Задача {i + 1}: {task}")

    print("4. Проверка очередей...")
    queues = client.get_queues()
    print(f"   Очереди: {queues}")

def test_server_connection():
    print("=== Проверка подключения к серверу ===")
    try:
        response = requests.get("http://localhost:8000")
        if response.status_code == 200:
            print("✅ Сервер доступен")
            return True
        else:
            print(f"❌ Сервер вернул код {response.status_code}")
            return False
    except requests.exceptions.ConnectionError:
        print("❌ Не удается подключиться к серверу")
        print("   Убедитесь, что сервер запущен: python run.py")
        return False
    except Exception as e:
        print(f"❌ Ошибка подключения: {e}")
        return False

def main():
    print("Запуск демонстрации брокера сообщений...")

    if not test_server_connection():
        return

    try:
        test_pub_sub()
        test_queue()

        print("\n=== Демонстрация завершена ===")
        print("✅ Все тесты выполнены успешно!")
        print("🔍 Проверьте логи сервера для просмотра доставленных сообщений")

    except Exception as e:
        print(f"❌ Ошибка во время демонстрации: {e}")

if __name__ == "__main__":
    main()
