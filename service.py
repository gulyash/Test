import pika
import logging
import json

from dateutil import parser


LOGGER = logging.getLogger(__name__)


def job_profit(job):
    completion_time = parser.parse(job['completion_time'])
    start_time = parser.parse(job['start_time'])
    delta_time = (completion_time - start_time).total_seconds()
    profit = delta_time/3600*job['nodes_used']*job['passmark']/15000
    result = {'id': job['id'],
              'profit': profit}
    return result


def count_profits(jobs):
    response = []
    for job in jobs:
        jp = job_profit(job)
        response.append(jp)
    return response


def on_request(self, method, props, body):
    request = json.loads(body)
    response = []
    if request['method'] == 'count_profits':
        response = count_profits((request['args'])[0])
        response_json = json.dumps(response, sort_keys=True)
        self.basic_publish(exchange='',
                           routing_key=SERVER_QUEUE,
                           properties=pika.BasicProperties(correlation_id=props.correlation_id),
                           body=response_json)

    self.basic_ack(delivery_tag=method.delivery_tag)
    return response


CLIENT_QUEUE = 'client_queue'  # From the core to the service.
SERVER_QUEUE = 'server_queue'  # From the service to the core.


connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))

channel = connection.channel()

channel.queue_declare(queue=CLIENT_QUEUE)
channel.queue_declare(queue=SERVER_QUEUE)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(on_request,
                      queue=CLIENT_QUEUE)
channel.start_consuming()
