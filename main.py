import requests
import pandas as pd

# Requisição de dados
df = requests.get(
    "https://sidra.ibge.gov.br/Ajax/JSon/Tabela/1/1737?versao=-1"
)
df = df.json()

# Extrair da resposta conjunto de dados 1 - Períodos
df1 = pd.json_normalize(df["Periodos"]["Periodos"])


# Função 1 - Retornar conjunto de período correspondente
def periodo_pertence_ao_conjunto(periodo: int) -> str:
    for conjunto in df["Periodos"]["Conjuntos"]:
        if periodo in conjunto["Periodos"]:
            return conjunto["Nome"]


# Aplicar Função 1: nome do conjunto de períodos ao dataset de períodos
df1["Conjunto"] = df1["Id"].apply(
    lambda periodo: periodo_pertence_ao_conjunto(periodo)
)


# Função 2 - Criar várias colunas quando dados relevantes tem formato de lista
def adicionar_colunas_para_dados_em_lista(
    df1: pd.DataFrame, df: list, nome_lista: str
) -> pd.DataFrame:
    for i, var in enumerate(df):
        df1[f"{nome_lista}{i+1}"] = var
    return df1


# Adicionar demais variáveis relevantes de metadados ao formato tabular
df1["DataAtualizacao"] = df["DataAtualizacao"]
df1["Nome_Pesquisa"] = df["Nome"]
df1 = adicionar_colunas_para_dados_em_lista(
    df1, df["Notas"], nome_lista="Nota"
)
df1["Pesquisa"] = df["Pesquisa"]["Nome"]
df1["UrlSidra"] = df["Pesquisa"]["UrlSidra"]
df1 = adicionar_colunas_para_dados_em_lista(
    df1,
    df["Territorios"]["DicionarioNiveis"]["Nomes"],
    nome_lista="Territorio",
)


# Função 3 - Tratar e renomear coluna de data (de string para data)
def tratar_data(df: pd.DataFrame, nome_data_antigo: str) -> pd.DataFrame:
    df = df.rename(columns={nome_data_antigo: "Data"})
    df[["Mês - Split 1", "Mês - Split 2"]] = df["Data"].str.split(
        pat=" ", expand=True
    )
    mes_dict = {
        "janeiro": "01",
        "fevereiro": "02",
        "março": "03",
        "abril": "04",
        "maio": "05",
        "junho": "06",
        "julho": "07",
        "agosto": "08",
        "setembro": "09",
        "outubro": "10",
        "novembro": "11",
        "dezembro": "12",
    }
    df["Mês ajustado"] = df["Mês - Split 1"].map(mes_dict)
    df["Data"] = df["Mês ajustado"] + "/01/" + df["Mês - Split 2"]
    df = df.drop(columns=["Mês ajustado", "Mês - Split 2", "Mês - Split 1"])
    df["Data"] = df["Data"].astype("datetime64[ns]").dt.date
    return df


# Aplicar Função 3: Tratar data
df1 = tratar_data(df1, nome_data_antigo="Nome")

# Salvar dados finais em formato Parquet
df1.to_parquet("DADOS_IPCA.parquet")
