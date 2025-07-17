import boto3
import json
import yaml

def parse_yml(database_name: str, table_name: str):
    tabela = f"{database_name}.{table_name}"

    with open("/home/labs/Documents/dev/teste_eng_dados_2/4.Data Quality/rule_table.yml", "r") as f:
        config = yaml.safe_load(f)

    for tb in config['tabelas']:
        if tb['nome_tabela'] == tabela:
            return tb['rule_path']
    return None

def retrieve_dqdl_ruleset(path):
  with open(path) as f:
    json_data = json.load(f)
    return json_data

def create_rule(rule_name: str, database_name: str, table_name: str):
    try:
        response = glue_client.create_data_quality_ruleset(
            Name=f'{rule_name}',
            Description=f'{description}',
            Ruleset=retrieve_dqdl_ruleset(f"{rule_path}"),
            TargetTable={
                'TableName': f'{table_name}',
                'DatabaseName': f'{database_name}'
            },

        )
        print(f"Ruleset created successfully: {response}")

    except Exception as e:
        print(f"Error creating ruleset: {e}")


args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

rule_path = parse_yml("silver_clientes", "tabela_clientes")
json_rule = retrieve_dqdl_ruleset(rule_path)
create_rule(rule_name="Regra tabela silver clientes", database_name="silver_clientes", table_name="tabela_clientes")

job.commit()