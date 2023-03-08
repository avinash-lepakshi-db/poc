from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDataOperator


from airflow.models import Variable,DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS']='prefab-shape-376611-01e4d96b8895.json'

# from bq_handler import BQ_HANDLER
# from sftp_handler import IO_HANDLER

# def BigQueryGetDataOperator(**kwargs):
#     S='BigQueryGetDataOperator('
#     for k,v in kwargs.items():
#         S+='{}={}'.format(k,v)
#     S=S[:-1]
#     return S+')'

def handler_name_template(name):
    return '__airflow_{}_operator__'.format(name)

# class DAG:
#     def __init__(self, *args,config_val=None, **kwargs):
#         for k, v in kwargs.items():
#             if 'config_val' in v :
#                 print(k,v)
#                 setattr(self,k,eval(v))
#             else:
#                 setattr(self, k, v)

#     def who_am_i(self):
#         return 'IM A DAG'
    

class BQ_HANDLER():
    def __init__(self,task_dict, task_id,**kwargs):
        self.task_id = task_id
        # self.config_val=config_val
        for k,v in task_dict.items():
            setattr(self, k, v)
        

    def __airflow_dataload_operator__(self):
        return BigQueryGetDataOperator(
            task_id=self.task_id,
            dataset_id=self.dataset_id,
            table_id=self.table_id
        )

class DUMMY():
    def __init__(self,task_dict, task_id,**kwargs):
        self.task_id = task_id
        # self.config_val=config_val
        for k,v in task_dict.items():
            setattr(self, k, v)

    def __airflow_dummy_operator__(self):
        return DummyOperator(
            task_id=self.task_id
        )



class DagGen(DAG):
    def __init__(self, *args, config_val=None, **kwargs):
        # super().__init__(*args, **kwargs)
        self.config_val=config_val
        for k,v in kwargs.items():
            setattr(self,k,v)
        
        # self.task_dict=self.config_val['tasks']

        for k,v in self.config_val.items():
            if k not in ('tasks','graph'):
                setattr(self, k, v)
            # elif k in ('graph','start_date','end_date'):
            #     print(k,v)
            #     setattr(self,k,exec('{v}'))
    # def __constructer_task__(self):
    #     for k,v in self.task_dict.items():
    #         yield exec('{}={}'.format(k,v))
    #     return 

    def dag_gen(self,**kwargs):
        # self.TASKS = {}
        for k,v in self.config_val['tasks'].items():
            if '__airflow_{}_operator__'.format(v['TYPE']) in dir(BQ_HANDLER):

                obj= BQ_HANDLER(task_id = k,task_dict=v)
                setattr(self,k,obj.__airflow_dataload_operator__())
            else:
                obj=DUMMY(task_dict=v, task_id=k)
                setattr(self,k,obj.__airflow_dummy_operator__())
            print(obj)
        print(dir(A))
            # return exec('{}={}'.format(k,''))
            
        
    def graph(self):
        setattr(self,'graph',config_val['graph'])

    #     self.config_val = config_val
        
    #     self.task_dict = config_val['tasks']

    # def dag_gen(config_val=None,**kwargs):
    #     for k,v in config_val['tasks']:
    #         return exec('{}={}'.format(k,''))

    # def _method_selector_(self, name):
    #     handlers = [i for i in dir(__name__) if 'HANDLER' in i]
    #     for h in handlers:
    #         if handler_name_template(name) in dir(getattr(__name__, h)):
    #             return getattr('__name__.{}'.format(h), handler_name_template(name))
    #     return None

    # def __gen_task__(self, task_id = None, task_dict = None, config_val = None):
        
    #     OBJ = self._method_selector_(task_dict['TYPE'])(task_id = None, task_dict = None, **config_val)
        
    #     exec('{} = {}'.fromat(task_id,'OBJ.get_task()'))
    #     self.task_dict[task_id] = getattr(__name__, task_id)

    # def gen_dag(self, config_val,**kwargs):
    #     for k, v in config_val['tasks'].items():
    #         # _task_dict = __variable_overload__(v, **kwargs)
    #         self.__gen_task__(task_id=k, task_dict= _task_dict, **config_val)

    # def get_task(self, name):
    #     assert name in self.task_dict.keys(), "Task {} not assigned in DAG".format(name) 
    #     return self.task_dict[name]

    # def evaluate(self):
    #     exec(config_val['graph'])

    # def __variable_overload__(task_dict, **kwargs):
    #     for k, v in kwargs.items:
    #         if k in task_dict.keys():
    #             task_dict[k] = v
    #     return task_dict


if __name__ == '__main__':

    # # dag = DAG(dag_id ......)
    dag_kwarg={

            "default_args":{
                "owner":"config_val['composer']",
                "retires":"config_val['retires']",
                "depends_on_past":"config_val['depends_on_past']"
            },
            "environent":{
                "tempLocation":"config_val['temp']",
                "subnetwork":"config_val['SUBNETWORK']"
            }
            }
    test_kwargs={
                             "dag_id":"test",
            "schdule_interval":"@once",
            "composer":"owner",
            
            "start_date":"pendulum.datetime(2023, 3, 8)",
    }
    config_val={
            "dag_id":"test",
            "schdule_interval":"@once",
            "composer":"owner",
            "start_date":"datetime(2023, 3, 8)",
            "end_date":"datetime(2023, 3, 8)",
            "tasks":{
                 "__init__":{
                    "task_id":"UB",
                    "TYPE":"dataload",
                    "dataset_id":"d1",
                    "table_id":"t1"
                },
                "start":{
                    "task_id":"d1",
                    "TYPE":"dummy"
                }
            },

            "graph":"start >> task1"
    }

    A = DagGen(config_val=config_val)
    A.dag_gen()
    print(dir(A.dag_gen()))
    # A.graph()

    # A.gen_dag(config_val)


    #         "dags":{
    #             "dag1":{"dag_id":"d1",
    #             "schedule_interval":"@once",
    #             "task1":{
    #                 "FEED_NAME":"UB",
    #                 "TABLE_NAME":"caramel-duality-364409.test.test1",
    #                 "REGION":"us-west2 ",
    #                 "PROJECT":"caramel-duality-364409",
    #                 "DATASET_NAME":"test",
    #                 "LIMIT":20,
    #                 "SQL_FILE":"./queries.sql",
    #                 "COLUMNS":"",
    #                 "TYPE":"select_query"
    #             }
    #         }
    #         }
        
    #     }

    # 'function to find all the files in a directory'


    # dag_list = {
    #     'dag_1' : {'dag_id': 'dag11',
    #                 'variable_overloading' : {'file_name':'file_1'}},
    #     'dag_2' : {'dag_id': 'dag12',
    #                 'variable_overloading' : {'file_name':'file_2'}},
    #     'dag_3' : {'dag_id': 'dag13',
    #                 'variable_overloading' : {'file_name':'file_3'}}
    #             }
    # dag=df_dag(*config)

    # for k, v  in config.items():
        
    #     dag.gen_dag(tasks, config_val, **v['variable_overloading'])
    #     dag.evaluate()
