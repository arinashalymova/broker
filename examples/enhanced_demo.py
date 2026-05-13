"""
Демонстрация расширенной функциональности брокера
"""
import requests
import json
import time
from datetime import datetime

class EnhancedBrokerDemo:
    def __init__(self, base_url: str = "http://localhost:8000/api/v1"):
        self.base_url = base_url

    def demo_priorities(self):
        """Демонстрация приоритетов сообщений"""
        print("🚀 === ДЕМОНСТРАЦИЯ ПРИОРИТЕТОВ СООБЩЕНИЙ ===")

        # Создаем топик
        print("1. Создание топика для демо приоритетов...")
        requests.post(f"{self.base_url}/topics", json={"name": "priority-demo"})

        # Создаем подписку
        requests.post(f"{self.base_url}/subscriptions", json={
            "subscriber_id": "priority-subscriber",
            "target_type": "topic",
            "target_name": "priority-demo"
        })

        print("2. Публикация сообщений с разными приоритетами...")

        # Сообщения в обратном порядке приоритета
        messages = [
            ("Обычное сообщение", "NORMAL"),
            ("Критичная ошибка системы!", "CRITICAL"),
            ("Низкий приоритет", "LOW"),
            ("Высокий приоритет", "HIGH"),
            ("Еще обычное", "NORMAL")
        ]

        for i, (content, priority) in enumerate(messages, 1):
            payload = {
                "content": {"text": content, "id": i},
                "priority": priority,
                "headers": {"demo": "priorities"}
            }

            response = requests.post(f"{self.base_url}/topics/priority-demo/publish", json=payload)
            if response.status_code == 200:
                print(f"   ✅ {priority:8}: {content}")
            else:
                print(f"   ❌ {priority:8}: Ошибка")

            time.sleep(0.5)

        print("   💡 Обратите внимание на порядок обработки в логах сервера!")

    def demo_ttl(self):
        """Демонстрация TTL (Time To Live)"""
        print("\n⏰ === ДЕМОНСТРАЦИЯ TTL (TIME TO LIVE) ===")

        # Создаем топик
        requests.post(f"{self.base_url}/topics", json={"name": "ttl-demo"})

        print("1. Публикация сообщений с разным TTL...")

        # Сообщения с разным TTL
        ttl_messages = [
            ("Сообщение без TTL", None),
            ("TTL 10 секунд", 10),
            ("TTL 5 секунд", 5),
            ("TTL 30 секунд", 30)
        ]

        for content, ttl in ttl_messages:
            payload = {
                "content": {"text": content, "timestamp": datetime.now().isoformat()},
                "ttl_seconds": ttl,
                "headers": {"demo": "ttl"}
            }

            response = requests.post(f"{self.base_url}/topics/ttl-demo/publish", json=payload)
            if response.status_code == 200:
                ttl_str = f"{ttl}s" if ttl else "∞"
                print(f"   ✅ TTL {ttl_str:4}: {content}")

        print("2. Ожидание истечения TTL...")
        for i in range(15):
            print(f"   ⏳ {i+1}/15 секунд...")
            time.sleep(1)

        print("   💡 Проверьте логи сервера на предмет очистки истекших сообщений!")

    def demo_dlq(self):
        """Демонстрация Dead Letter Queue"""
        print("\n☠️ === ДЕМОНСТРАЦИЯ DEAD LETTER QUEUE ===")

        print("1. Проверка текущего состояния DLQ...")
        response = requests.get(f"{self.base_url}/dlq")
        if response.status_code == 200:
            dlq_data = response.json()
            print(f"   📊 DLQ: {dlq_data['message_count']} сообщений")

            if dlq_data['message_count'] > 0:
                print("2. Сообщения в DLQ:")
                for msg in dlq_data.get('messages', []):
                    reason = msg.get('headers', {}).get('dlq_reason', 'unknown')
                    print(f"     - {msg['id']}: {reason}")

                print("3. Очистка DLQ...")
                clear_response = requests.delete(f"{self.base_url}/dlq")
                if clear_response.status_code == 200:
                    print("   ✅ DLQ очищена")
            else:
                print("   ✅ DLQ пуста (это хорошо!)")
        else:
            print(f"   ❌ Ошибка получения DLQ: {response.status_code}")

    def demo_metrics(self):
        """Демонстрация метрик"""
        print("\n📊 === ДЕМОНСТРАЦИЯ МЕТРИК ===")

        response = requests.get(f"{self.base_url}/metrics")
        if response.status_code == 200:
            metrics = response.json()

            print("1. Метрики сообщений:")
            messages = metrics.get("messages", {})
            for key, value in messages.items():
                print(f"   {key:15}: {value}")

            print("\n2. Метрики сущностей:")
            entities = metrics.get("entities", {})
            for key, value in entities.items():
                print(f"   {key:15}: {value}")

            print("\n3. Метрики производительности:")
            performance = metrics.get("performance", {})
            for key, value in performance.items():
                if isinstance(value, float):
                    print(f"   {key:20}: {value:.2f}")
                else:
                    print(f"   {key:20}: {value}")

            print(f"\n4. Время запуска: {metrics.get('started_at', 'unknown')}")
        else:
            print(f"   ❌ Ошибка получения метрик: {response.status_code}")

    def demo_config(self):
        """Демонстрация конфигурации"""
        print("\n⚙️ === ДЕМОНСТРАЦИЯ КОНФИГУРАЦИИ ===")

        response = requests.get(f"{self.base_url}/config")
        if response.status_code == 200:
            config = response.json()

            print("Текущая конфигурация брокера:")
            important_params = [
                "max_topics", "enable_dlq", "default_message_ttl",
                "max_retry_attempts", "cleanup_interval", "dlq_max_size"
            ]

            for param in important_params:
                if param in config:
                    print(f"   {param:20}: {config[param]}")
        else:
            print(f"   ❌ Ошибка получения конфигурации: {response.status_code}")

def main():
    """Основная демонстрация"""
    print("🎭 РАСШИРЕННАЯ ДЕМОНСТРАЦИЯ БРОКЕРА СООБЩЕНИЙ")
    print("=" * 70)
    print(f"Время запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Проверяем доступность сервера
    try:
        response = requests.get("http://localhost:8000/api/v1/health", timeout=5)
        if response.status_code != 200:
            print("❌ Сервер недоступен. Запустите: python run.py")
            return
    except:
        print("❌ Не удается подключиться к серверу")
        return

    demo = EnhancedBrokerDemo()

    try:
        # Запускаем все демонстрации
        demo.demo_config()
        demo.demo_metrics()
        demo.demo_priorities()
        demo.demo_ttl()
        demo.demo_dlq()

        print("\n" + "=" * 70)
        print("🎉 ВСЕ ДЕМОНСТРАЦИИ ЗАВЕРШЕНЫ!")
        print("\n🔗 Полезные ссылки:")
        print("📚 Swagger API: http://localhost:8000/api/v1/docs")
        print("💚 Health Check: http://localhost:8000/api/v1/health")
        print("📊 Метрики: http://localhost:8000/api/v1/metrics")
        print("☠️ Dead Letter Queue: http://localhost:8000/api/v1/dlq")
        print("⚙️ Конфигурация: http://localhost:8000/api/v1/config")

    except KeyboardInterrupt:
        print("\n\n⏹️ Демонстрация прервана пользователем")
    except Exception as e:
        print(f"\n\n❌ Ошибка во время демонстрации: {e}")

if __name__ == "__main__":
    main()
