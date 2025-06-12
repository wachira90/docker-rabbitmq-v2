# ตัวอย่าง RabbitMQ 3.13.7 สำหรับประมวลผลคำสั่งซื้อ
# ติดตั้งแพ็คเกจที่จำเป็น: pip install pika==1.3.2

import pika
import json
import time
import sys
from datetime import datetime

# ขั้นตอนที่ 1: การตั้งค่าการเชื่อมต่อ
def create_connection():
    """สร้างการเชื่อมต่อไปยังเซิร์ฟเวอร์ RabbitMQ"""
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters('localhost')
        )
        channel = connection.channel()
        print("✅ เชื่อมต่อ RabbitMQ สำเร็จ")
        return connection, channel
    except Exception as e:
        print(f"❌ ไม่สามารถเชื่อมต่อ RabbitMQ: {e}")
        sys.exit(1)

# ขั้นตอนที่ 2: การประกาศคิว
def setup_queues(channel):
    """ประกาศคิวสำหรับขั้นตอนการประมวลผลต่างๆ"""
    
    # คิวคำสั่งซื้อหลัก
    channel.queue_declare(queue='order_queue', durable=True)
    
    # คิวการประมวลผล
    channel.queue_declare(queue='payment_queue', durable=True)
    channel.queue_declare(queue='inventory_queue', durable=True)
    channel.queue_declare(queue='shipping_queue', durable=True)
    channel.queue_declare(queue='notification_queue', durable=True)
    
    print("✅ ประกาศคิวเรียบร้อยแล้ว")

# ขั้นตอนที่ 3: ผู้ส่งคำสั่งซื้อ (เว็บแอปพลิเคชัน)
class OrderProducer:
    def __init__(self, channel):
        self.channel = channel
    
    def publish_order(self, order_data):
        """เผยแพร่คำสั่งซื้อใหม่ไปยังคิวคำสั่งซื้อ"""
        message = json.dumps(order_data)
        
        self.channel.basic_publish(
            exchange='',
            routing_key='order_queue',
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=2,  # ทำให้ข้อความคงอยู่
            )
        )
        print(f"📦 คำสั่งซื้อ {order_data['order_id']} ส่งไปยังคิวแล้ว")

# ขั้นตอนที่ 4: ผู้รับคำสั่งซื้อ (บริการประมวลผลคำสั่งซื้อ)
class OrderProcessor:
    def __init__(self, channel):
        self.channel = channel
        
    def process_order(self, ch, method, properties, body):
        """ประมวลผลคำสั่งซื้อที่เข้ามา"""
        try:
            order = json.loads(body)
            print(f"🔄 กำลังประมวลผลคำสั่งซื้อ {order['order_id']}")
            
            # จำลองการตรวจสอบคำสั่งซื้อ
            time.sleep(1)
            
            # ส่งต่อไปยังบริการต่างๆ ตามคำสั่งซื้อ
            if order['status'] == 'pending':
                self.route_to_payment(order)
                
            # ยืนยันการประมวลผลข้อความ
            ch.basic_ack(delivery_tag=method.delivery_tag)
            print(f"✅ คำสั่งซื้อ {order['order_id']} ประมวลผลสำเร็จ")
            
        except Exception as e:
            print(f"❌ เกิดข้อผิดพลาดในการประมวลผลคำสั่งซื้อ: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
    
    def route_to_payment(self, order):
        """ส่งคำสั่งซื้อไปยังการประมวลผลการชำระเงิน"""
        payment_message = {
            'order_id': order['order_id'],
            'amount': order['total'],
            'customer_id': order['customer_id'],
            'timestamp': datetime.now().isoformat()
        }
        
        self.channel.basic_publish(
            exchange='',
            routing_key='payment_queue',
            body=json.dumps(payment_message),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        print(f"💳 คำสั่งซื้อ {order['order_id']} ส่งไปยังการประมวลผลการชำระเงิน")

# ขั้นตอนที่ 5: ตัวประมวลผลการชำระเงิน
class PaymentProcessor:
    def __init__(self, channel):
        self.channel = channel
    
    def process_payment(self, ch, method, properties, body):
        """ประมวลผลการชำระเงินสำหรับคำสั่งซื้อ"""
        try:
            payment_data = json.loads(body)
            print(f"💳 กำลังประมวลผลการชำระเงินสำหรับคำสั่งซื้อ {payment_data['order_id']}")
            
            # จำลองการประมวลผลการชำระเงิน
            time.sleep(2)
            
            # จำลองความสำเร็จในการชำระเงิน (อัตราความสำเร็จ 90%)
            import random
            if random.random() > 0.1:
                print(f"✅ ชำระเงินสำเร็จสำหรับคำสั่งซื้อ {payment_data['order_id']}")
                self.route_to_inventory(payment_data)
            else:
                print(f"❌ การชำระเงินล้มเหลวสำหรับคำสั่งซื้อ {payment_data['order_id']}")
                self.send_failure_notification(payment_data)
            
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
        except Exception as e:
            print(f"❌ เกิดข้อผิดพลาดในการประมวลผลการชำระเงิน: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
    
    def route_to_inventory(self, payment_data):
        """ส่งไปตรวจสอบสต็อกหลังจากชำระเงินสำเร็จ"""
        inventory_message = {
            'order_id': payment_data['order_id'],
            'customer_id': payment_data['customer_id'],
            'status': 'paid',
            'timestamp': datetime.now().isoformat()
        }
        
        self.channel.basic_publish(
            exchange='',
            routing_key='inventory_queue',
            body=json.dumps(inventory_message),
            properties=pika.BasicProperties(delivery_mode=2)
        )
    
    def send_failure_notification(self, payment_data):
        """ส่งการแจ้งเตือนสำหรับการชำระเงินที่ล้มเหลว"""
        notification = {
            'type': 'payment_failed',
            'order_id': payment_data['order_id'],
            'customer_id': payment_data['customer_id'],
            'message': 'การประมวลผลการชำระเงินล้มเหลว กรุณาลองอีกครั้ง'
        }
        
        self.channel.basic_publish(
            exchange='',
            routing_key='notification_queue',
            body=json.dumps(notification),
            properties=pika.BasicProperties(delivery_mode=2)
        )

# ขั้นตอนที่ 6: ตัวอย่างการใช้งาน
def run_producer_demo():
    """ตัวอย่าง: เผยแพร่คำสั่งซื้อตัวอย่าง"""
    connection, channel = create_connection()
    setup_queues(channel)
    
    producer = OrderProducer(channel)
    
    # คำสั่งซื้อตัวอย่าง
    orders = [
        {
            'order_id': 'ORD-001',
            'customer_id': 'CUST-123',
            'items': ['laptop', 'mouse'],
            'total': 999.99,
            'status': 'pending',
            'timestamp': datetime.now().isoformat()
        },
        {
            'order_id': 'ORD-002',
            'customer_id': 'CUST-456',
            'items': ['phone', 'case'],
            'total': 599.99,
            'status': 'pending',
            'timestamp': datetime.now().isoformat()
        }
    ]
    
    for order in orders:
        producer.publish_order(order)
        time.sleep(1)
    
    connection.close()
    print("🏁 การสาธิตผู้ผลิตเสร็จสิ้น")

def run_consumer_demo():
    """ตัวอย่าง: รับและประมวลผลคำสั่งซื้อ"""
    connection, channel = create_connection()
    setup_queues(channel)
    
    # ตั้งค่าตัวประมวลผล
    order_processor = OrderProcessor(channel)
    payment_processor = PaymentProcessor(channel)
    
    # กำหนดค่าผู้บริโภค
    channel.basic_qos(prefetch_count=1)  # ประมวลผลหนึ่งข้อความต่อครั้ง
    
    # ตั้งค่าผู้บริโภคคำสั่งซื้อ
    channel.basic_consume(
        queue='order_queue',
        on_message_callback=order_processor.process_order
    )
    
    # ตั้งค่าผู้บริโภคการชำระเงิน
    channel.basic_consume(
        queue='payment_queue',
        on_message_callback=payment_processor.process_payment
    )
    
    print("🔄 เริ่มต้นผู้บริโภค... กด CTRL+C เพื่อออก")
    
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\n🛑 หยุดผู้บริโภค...")
        channel.stop_consuming()
        connection.close()

# ขั้นตอนที่ 7: การรันหลัก
if __name__ == "__main__":
    print("ตัวอย่าง RabbitMQ 3.13.7 สำหรับประมวลผลคำสั่งซื้อ")
    print("===============================================")
    
    if len(sys.argv) > 1 and sys.argv[1] == "producer":
        run_producer_demo()
    elif len(sys.argv) > 1 and sys.argv[1] == "consumer":
        run_consumer_demo()
    else:
        print("วิธีใช้:")
        print("  python script.py producer  # รันผู้ผลิตคำสั่งซื้อ")
        print("  python script.py consumer  # รันผู้บริโภคคำสั่งซื้อ")
        print("\nรันผู้ผลิตก่อน จากนั้นรันผู้บริโภคในเทอร์มินัลแยกต่างหาก")