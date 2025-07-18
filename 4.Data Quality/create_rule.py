import boto3
import json
import yaml

class DataQualityManager:
    def __init__(self, config_path: str):
        self.config_path = config_path
        self.glue_client = boto3.client('glue')

    def parse_yml(self, database_name: str, table_name: str) -> str | None:
        tabela = f"{database_name}.{table_name}"

        with open(self.config_path, "r") as f:
            config = yaml.safe_load(f)

        for tb in config.get('tabelas', []):
            if tb.get('nome_tabela') == tabela:
                return tb.get('rule_path')
        return None

    def retrieve_dqdl_ruleset(self, path: str) -> dict:
        with open(path, "r") as f:
            return json.load(f)

    def create_rule(self, rule_name: str, database_name: str, table_name: str, description: str):
        rule_path = self.parse_yml(database_name, table_name)

        if rule_path is None:
            print(f"Nenhuma regra encontrada para {database_name}.{table_name}")
            return

        try:
            ruleset = self.retrieve_dqdl_ruleset(rule_path)

            response = self.glue_client.create_data_quality_ruleset(
                Name=rule_name,
                Description=description,
                Ruleset=ruleset,
                TargetTable={
                    'TableName': table_name,
                    'DatabaseName': database_name
                }
            )
            print(f"Ruleset criado com sucesso: {response}")

        except Exception as e:
            print(f"Erro ao criar o ruleset: {e}")


args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

dq_manager = DataQualityManager("/caminho/para/arquivo.yml")
dq_manager.create_rule(
    rule_name="Regra tabela silver clientes",
    database_name="silver_clientes",
    table_name="tabela_clientes",
    description="Validações básicas de qualidade"
)

job.commit()