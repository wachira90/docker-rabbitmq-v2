# RabbitMQ 3.13.7 Order Processing Example
# Install required package: pip install pika==1.3.2

import pika
import json
import time
import sys
from datetime import datetime

# Step 1: Connection Setup
def create_connection():
    """Create connection to RabbitMQ server"""
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters('localhost')
        )
        channel = connection.channel()
        print("✅ Connected to RabbitMQ")
        return connection, channel
    except Exception as e:
        print(f"❌ Failed to connect to RabbitMQ: {e}")
        sys.exit(1)

# Step 2: Queue Declaration
def setup_queues(channel):
    """Declare queues for different processing stages"""
    
    # Main order queue
    channel.queue_declare(queue='order_queue', durable=True)
    
    # Processing queues
    channel.queue_declare(queue='payment_queue', durable=True)
    channel.queue_declare(queue='inventory_queue', durable=True)
    channel.queue_declare(queue='shipping_queue', durable=True)
    channel.queue_declare(queue='notification_queue', durable=True)
    
    print("✅ Queues declared successfully")

# Step 3: Order Producer (Web Application)
class OrderProducer:
    def __init__(self, channel):
        self.channel = channel
    
    def publish_order(self, order_data):
        """Publish new order to the order queue"""
        message = json.dumps(order_data)
        
        self.channel.basic_publish(
            exchange='',
            routing_key='order_queue',
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=2,  # Make message persistent
            )
        )
        print(f"📦 Order {order_data['order_id']} published to queue")

# Step 4: Order Consumer (Order Processing Service)
class OrderProcessor:
    def __init__(self, channel):
        self.channel = channel
        
    def process_order(self, ch, method, properties, body):
        """Process incoming orders"""
        try:
            order = json.loads(body)
            print(f"🔄 Processing order {order['order_id']}")
            
            # Simulate order validation
            time.sleep(1)
            
            # Route to different services based on order
            if order['status'] == 'pending':
                self.route_to_payment(order)
                
            # Acknowledge message processing
            ch.basic_ack(delivery_tag=method.delivery_tag)
            print(f"✅ Order {order['order_id']} processed successfully")
            
        except Exception as e:
            print(f"❌ Error processing order: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
    
    def route_to_payment(self, order):
        """Route order to payment processing"""
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
        print(f"💳 Order {order['order_id']} routed to payment processing")

# Step 5: Payment Processor
class PaymentProcessor:
    def __init__(self, channel):
        self.channel = channel
    
    def process_payment(self, ch, method, properties, body):
        """Process payment for order"""
        try:
            payment_data = json.loads(body)
            print(f"💳 Processing payment for order {payment_data['order_id']}")
            
            # Simulate payment processing
            time.sleep(2)
            
            # Simulate payment success (90% success rate)
            import random
            if random.random() > 0.1:
                print(f"✅ Payment successful for order {payment_data['order_id']}")
                self.route_to_inventory(payment_data)
            else:
                print(f"❌ Payment failed for order {payment_data['order_id']}")
                self.send_failure_notification(payment_data)
            
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
        except Exception as e:
            print(f"❌ Payment processing error: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
    
    def route_to_inventory(self, payment_data):
        """Route to inventory check after successful payment"""
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
        """Send notification for payment failure"""
        notification = {
            'type': 'payment_failed',
            'order_id': payment_data['order_id'],
            'customer_id': payment_data['customer_id'],
            'message': 'Payment processing failed. Please try again.'
        }
        
        self.channel.basic_publish(
            exchange='',
            routing_key='notification_queue',
            body=json.dumps(notification),
            properties=pika.BasicProperties(delivery_mode=2)
        )

# Step 6: Demo Usage
def run_producer_demo():
    """Demo: Publish sample orders"""
    connection, channel = create_connection()
    setup_queues(channel)
    
    producer = OrderProducer(channel)
    
    # Sample orders
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
    print("🏁 Producer demo completed")

def run_consumer_demo():
    """Demo: Consume and process orders"""
    connection, channel = create_connection()
    setup_queues(channel)
    
    # Set up processors
    order_processor = OrderProcessor(channel)
    payment_processor = PaymentProcessor(channel)
    
    # Configure consumers
    channel.basic_qos(prefetch_count=1)  # Process one message at a time
    
    # Set up order consumer
    channel.basic_consume(
        queue='order_queue',
        on_message_callback=order_processor.process_order
    )
    
    # Set up payment consumer
    channel.basic_consume(
        queue='payment_queue',
        on_message_callback=payment_processor.process_payment
    )
    
    print("🔄 Starting consumers... Press CTRL+C to exit")
    
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\n🛑 Stopping consumers...")
        channel.stop_consuming()
        connection.close()

# Step 7: Main execution
if __name__ == "__main__":
    print("RabbitMQ 3.13.7 Order Processing Demo")
    print("=====================================")
    
    if len(sys.argv) > 1 and sys.argv[1] == "producer":
        run_producer_demo()
    elif len(sys.argv) > 1 and sys.argv[1] == "consumer":
        run_consumer_demo()
    else:
        print("Usage:")
        print("  python script.py producer  # Run order producer")
        print("  python script.py consumer  # Run order consumer")
        print("\nRun producer first, then consumer in separate terminal")