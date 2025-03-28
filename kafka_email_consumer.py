import json
import logging
import os
import smtplib
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from dotenv import load_dotenv
from kafka import KafkaConsumer

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=os.getenv('LOG_LEVEL', 'INFO'),
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class EmailSender:
    def __init__(self):
        self.smtp_host = os.getenv('EMAIL_HOST', 'smtp.yandex.ru')
        self.smtp_port = int(os.getenv('EMAIL_PORT', '465'))
        self.smtp_user = os.getenv('EMAIL_USER', '')
        self.smtp_password = os.getenv('EMAIL_PASSWORD', '')
        self.from_email = os.getenv('FROM_EMAIL', os.getenv('EMAIL_USER', 'noreply@localhost'))
        
    def send_email(self, to_email, subject, message):
        """Отправка email через SMTP с SSL"""
        try:
            msg = MIMEMultipart()
            msg['From'] = self.from_email
            msg['To'] = to_email
            msg['Subject'] = subject
            msg.attach(MIMEText(message, 'plain'))
            
            # Для порта 465 используем SMTP_SSL вместо SMTP
            with smtplib.SMTP_SSL(self.smtp_host, self.smtp_port) as server:
                if self.smtp_user and self.smtp_password:
                    # Для SMTP_SSL starttls() не нужен
                    server.login(self.smtp_user, self.smtp_password)
                
                server.sendmail(self.from_email, to_email, msg.as_string())
            
            logger.info(f"Email successfully sent to {to_email}")
            return True
            
        except smtplib.SMTPException as e:
            logger.error(f"SMTP error sending to {to_email}: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error sending to {to_email}: {str(e)}")
        
        return False

def consume_messages():
    """Потребление сообщений из Kafka"""
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')  # Изменил на kafka:9092
    kafka_topic = os.getenv('KAFKA_TOPIC', 'email_notifications')
    group_id = os.getenv('KAFKA_GROUP_ID', 'email_sender_group')
    
    logger.info(f"Connecting to Kafka at {kafka_servers}")
    
    consumer_config = {
        'bootstrap_servers': kafka_servers,
        'group_id': group_id,
        'auto_offset_reset': 'earliest',
        'enable_auto_commit': False,
        'max_poll_records': 1,
        'session_timeout_ms': 30000,
        'heartbeat_interval_ms': 5000,
        'max_poll_interval_ms': 300000,
        'value_deserializer': lambda x: json.loads(x.decode('utf-8'))
    }
    
    email_sender = EmailSender()
    
    while True:  # Бесконечный цикл для переподключения
        consumer = None
        try:
            consumer = KafkaConsumer(**consumer_config)
            consumer.subscribe([kafka_topic])
            
            logger.info("Successfully connected to Kafka")
            
            while True:
                try:
                    # Получаем сообщения с таймаутом
                    records = consumer.poll(timeout_ms=1000)
                    
                    if not records:
                        continue
                        
                    for topic_partition, messages in records.items():
                        for message in messages:
                            try:
                                msg_value = message.value
                                logger.info(f"Processing message: {msg_value}")
                                
                                if not all(key in msg_value for key in ['to', 'subject', 'body']):
                                    logger.error("Invalid message format")
                                    continue
                                
                                # Отправка email
                                if email_sender.send_email(
                                    to_email=msg_value['to'],
                                    subject=msg_value['subject'],
                                    message=msg_value['body']
                                ):
                                    consumer.commit()
                                    logger.info(f"Successfully processed offset {message.offset}")
                                else:
                                    logger.error(f"Failed to process offset {message.offset}")
                                    
                            except Exception as e:
                                logger.error(f"Message processing error: {str(e)}")
                                
                except Exception as e:
                    logger.error(f"Polling error: {str(e)}")
                    break
                    
        except KeyboardInterrupt:
            logger.info("Shutting down...")
            break
        except Exception as e:
            logger.error(f"Connection error: {str(e)}. Reconnecting in 10s...")
            time.sleep(10)
        finally:
            if consumer:
                try:
                    consumer.close()
                except:
                    pass

if __name__ == '__main__':
    consume_messages()