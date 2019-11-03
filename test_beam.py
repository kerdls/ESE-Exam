import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import random
import requests

def _create_pipeline_options():
	pipeline_options = PipelineOptions.from_dictionary({
		'project': 'ese-exam-sandbox-delos-santos',
		'runner': 'DataflowRunner',
		'streaming': False,
		'temp_location': 'gs://ese-exam-sandbox-delos-santos/pipeline/dataflow-staging/temp',
		'staging_location': 'gs://ese-exam-sandbox-delos-santos/pipeline/dataflow-staging/staging',
		'job_name': 'exam-flow',
		'num_workers': 5,
		'region': 'us-central1',
		'autoscaling_algorithm': 'NONE',
		'worker_machine_type': 'n1-standard-2',
		'disk_size_gb': 100,
		'save_main_session': True,
		'setup_file': "./setup.py",
	})
	return pipeline_options

class FraudScore(beam.DoFn):
    def process(self,data):
    	data = data.split(',')
    	data={'cust_id':int(data[0]),
    		'item':int(data[1]),
    		'price':float(data[2]),
    		'time':int(data[3]),
    		'processingtime':int(data[4])
    	}
    	score = calculate_fraud(data['item'],data['price'],data['time'])
    	if score>0.4:
    		send_alert(data['item'],data['price'],data['time'],score)
        return dict(data,**{'fraudscore': score})

def calculate_fraud(item_id, price, transaction_time):
    return round(random.uniform(0,1),2)

def send_alert(item_id, price, transaction_time, score):
    requests.post(
		"https://api.mailgun.net/v3/sandbox2a0b791b36eb4136aaf2b3d29a909ea5.mailgun.org/messages",
		auth=("api", "60a979b6cdb19d9564cc36f3b1787b6b-816b23ef-33be9e4b"),
		data={"from": "Mailgun Sandbox <postmaster@sandbox2a0b791b36eb4136aaf2b3d29a909ea5.mailgun.org>",
			"to": "Kerwin Delos Santos <kerwin_0611@yahoo.com>",
			"subject": "Fraud Alert",
			"text": "Hello, \nA recent purchase was made with a high chance of fraud. The details are as follows:\n item:"+str(item_id)+"\nprice:"+str(price)+"\ntime:"+str(transaction_time)+"\nfraud_score:"+str(score)})


def main():
	schema = {'fields'=[{'name':'cust_id','type':'INTEGER'},
		'name':'item','type':'INTEGER'},
		'name':'price','type':'FLOAT'},
		'name':'time','type':'INTEGER'},
		'name':'processingtime','type':'INTEGER'},
		'name':'fraudscore','type':'FLOAT'}
	]}
	table = 'ese-exam-sandbox-delos-santos:amtcxm.transactions'

	pipeline_options = _create_pipeline_options()
	p = beam.Pipeline(options=pipeline_options)
	lines = (p | 'readfile'>>beam.io.ReadFromText('gs://ese-exam-sandbox-delos-santos/pipeline/data.csv',skip_header_lines=1) 
		| 'writetoconsole'>>beam.ParDo(FraudScore()) 
		| 'WriteBQ'>>beam.io.WriteToBigQuery(table,schema=schema,write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER)
		)
	p.run()


if __name__ == "__main__":
	main()
