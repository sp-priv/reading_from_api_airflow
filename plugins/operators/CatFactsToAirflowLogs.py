from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import requests
import json
import random
import multiprocessing
import logging as log


class CatFactsToAirflowLogs(BaseOperator):

    @apply_defaults
    def __init__(self, number_of_facts, animal_type, *args, **kwargs):

        self.number_of_facts = number_of_facts
        self.animal_type = animal_type
        super().__init__(*args, **kwargs)

    def execute(self, context):

        # get facts from api;
        # collect only verified facts (filters out entries that are not facts about cats)
        # return it as a dictionary and append to list where all facts will be stored
        def get_facts(list_to_append):
            number_of_facts = self.number_of_facts
            animal_type = self.animal_type
            cat_facts = {}
            url = f'https://cat-fact.herokuapp.com/facts/random?animal_type={animal_type}&amount={number_of_facts}'
            try:
                response = requests.get(url)
            except Exception as e:
                print(f'Unable to retrieve facts. Following error has occcured: \n {e}')

            cat_facts_all = json.loads(response.text)
            for element in cat_facts_all:
                if element['status']['verified']:
                    log.info(element["text"])
                    cat_facts[element["text"]] = cat_facts.setdefault(element["text"], 0) + 1
            list_to_append.append(cat_facts)

        # apply multiprocessing using get_facts method and load results into return_list
        manager = multiprocessing.Manager()
        return_list = manager.list()
        jobs = []

        for _ in range(20):
            p = multiprocessing.Process(target=get_facts, args=[return_list])
            jobs.append(p)
            p.start()

        for process in jobs:
            process.join()

        # combine facts  from dictionaries in return_list into one dictionary, count appearances
        results_dict = {}
        for sub in return_list:
            for key in sub:
                results_dict[key] = results_dict.setdefault(key, 0) + 1

        top_facts = sorted(results_dict.items(),
                           key=lambda x: x[1], reverse=True)

        max_number = top_facts[0][1]

        # get max number of facts appearances an select the random one (if there is only 1 it will be chosen anyway)
        max_number_facts = []
        for fact, appearances in top_facts:
            if appearances < max_number:
                break
            max_number_facts.append(str(fact))

        random_fact = random.choice(max_number_facts)

        return random_fact


class ApiPlugin(AirflowPlugin):
    name = "CatFactsToAirflowLogs"
    operators = [CatFactsToAirflowLogs]
    sensors = []
