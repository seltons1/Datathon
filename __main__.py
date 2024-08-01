import duckdb
import pandas as pd
import pandasql as psql
import gc

# Files paths.
RAW_PATH = 'raw/'
SILVER_PATH = 'silver/'

# Definition of states for analysis according to the IBGE code
CODIGO_IBGE_SIH = "330455,530010,355030"

CODIGO_IBGE = "3304557,5300108,3550308"

# List with all brazilian states for analyze. In this analyzes we'll be analyze this big three states.
ESTADOS_BRASILEIROS = [
    # 'AC',  # Acre
    # 'AL',  # Alagoas --
    # 'AP',  # Amapá
    # 'AM',  # Amazonas
    # 'BA',  # Bahia --
    # 'CE',  # Ceará --
    "DF",  # Distrito Federal
    # 'ES',  # Espírito Santo
    # 'GO',  # Goiás
    # 'MA',  # Maranhão --
    # 'MT',  # Mato Grosso
    # 'MS',  # Mato Grosso do Sul
    # 'MG',  # Minas Gerais
    # 'PA',  # Pará
    # 'PB',  # Paraíba --
    # 'PR',  # Paraná
    # 'PE',  # Pernambuco --
    # 'PI',  # Piauí --
    "RJ",  # Rio de Janeiro
    # 'RN',  # Rio Grande do Norte --
    # 'RS',  # Rio Grande do Sul
    # 'RO',  # Rondônia
    # 'RR',  # Roraima
    # 'SC',  # Santa Catarina
    "SP",  # São Paulo
    # 'SE',  # Sergipe --
    # 'TO'   # Tocantins
]

# Initial param
ANO_INICIAL = 2015
ANO_FINAL = 2021

def ler_acidentes_IPEA(conn) -> pd.DataFrame:
    """
    [Description]

        Reads traffic accident files by year and municipality, made available by IPEA.
        Numbers and rates of deaths from traffic accidents in Brazil, by year, municipality, sex. 
    
    [Source]

        Link: https://www.ipea.gov.br/atlasviolencia/filtros-series/12/violencia-no-transito

    [Goal]

        Reading of all files aggregated by year and municipality, making them available in a dataframe,
        seeking to obtain better performance in data analysis.
    
    """
    
    # Reading downloaded files using duckdb
    df_transportes = conn.execute(f"""SELECT * FROM '{RAW_PATH}IPEA/bitos-em-acidentes-de-transporte.csv' WHERE cod in ('3304557','5300108','3550308')""").df()
    df_mulheres = conn.execute(f"""SELECT * FROM '{RAW_PATH}IPEA/bitos-em-acidentes-de-transporte-mulheres.csv' WHERE cod in ('3304557','5300108','3550308')""").df()
    df_homens = conn.execute(f"""SELECT * FROM '{RAW_PATH}IPEA/bitos-em-acidentes-de-transporte-homens.csv' WHERE cod in ('3304557','5300108','3550308')""").df()
    df_jovens = conn.execute(f"""SELECT * FROM '{RAW_PATH}IPEA/bitos-em-acidentes-de-transporte-de-jovens.csv' WHERE cod in ('3304557','5300108','3550308')""").df()
    df_jovens_homens = conn.execute(f"""SELECT * FROM '{RAW_PATH}IPEA/bitos-em-acidentes-de-transporte-de-jovens-homens.csv' WHERE cod in ('3304557','5300108','3550308')""").df()
    df_jovens_mulheres = conn.execute(f"SELECT * FROM '{RAW_PATH}IPEA/bitos-em-acidentes-de-transporte-de-jovens-mulheres.csv' WHERE cod in ('3304557','5300108','3550308')""").df()
    df_tx_transportes = conn.execute(f"""SELECT * FROM '{RAW_PATH}IPEA/taxa-de-obitos-em-acidentes-de-transporte.csv' WHERE cod in ('3304557','5300108','3550308')""").df()
    df_tx_mulheres = conn.execute(f"""SELECT * FROM '{RAW_PATH}IPEA/taxa-de-obitos-em-acidentes-de-transporte-mulheres.csv' WHERE cod in ('3304557','5300108','3550308')""").df()
    df_tx_homens = conn.execute(f"""SELECT * FROM '{RAW_PATH}IPEA/taxa-de-obitos-em-acidentes-de-transporte-homens.csv' WHERE cod in ('3304557','5300108','3550308')""").df()
    df_tx_jovens = conn.execute(f"""SELECT * FROM '{RAW_PATH}IPEA/taxa-de-obitos-em-acidentes-de-transporte-de-jovens.csv' WHERE cod in ('3304557','5300108','3550308')""").df()
    df_tx_jovens_homens = conn.execute(f"""SELECT * FROM '{RAW_PATH}IPEA/taxa-de-obitos-em-acidentes-de-transporte-de-jovens-homens.csv' WHERE cod in ('3304557','5300108','3550308')""").df()
    df_tx_jovens_mulheres = conn.execute(f"""SELECT * FROM '{RAW_PATH}IPEA/taxa-de-obitos-em-acidentes-de-transporte-de-jovens-mulheres.csv' WHERE cod in ('3304557','5300108','3550308')""").df()

    # Query dataframes using to aggregate values ​​by month and year.
    query = """
        SELECT df_transportes.*, 
        df_mulheres.valor as total_mulheres, 
        df_homens.valor as total_homens, 
        df_jovens.valor as total_jovens, 
        df_jovens_homens.valor as total_jovens_homens, 
        df_jovens_mulheres.valor as total_jovens_mulheres,
        df_tx_transportes.valor as taxa_transporte,
        df_tx_mulheres.valor as taxa_mulheres,
        df_tx_homens.valor as taxa_homens,
        df_tx_jovens.valor as taxa_jovens,
        df_tx_jovens_homens.valor as taxa_jovens_homens,
        df_tx_jovens_mulheres.valor as taxa_jovens_mulheres


        FROM df_transportes
        LEFT JOIN df_mulheres
        ON df_transportes.cod = df_mulheres.cod AND df_transportes.período = df_mulheres.período
        LEFT JOIN df_homens
        ON df_transportes.cod = df_homens.cod AND df_transportes.período = df_homens.período
        LEFT JOIN df_jovens
        ON df_transportes.cod = df_jovens.cod AND df_transportes.período = df_jovens.período
        LEFT JOIN df_jovens_homens
        ON df_transportes.cod = df_jovens_homens.cod AND df_transportes.período = df_jovens_homens.período
        LEFT JOIN df_jovens_mulheres
        ON df_transportes.cod = df_jovens_mulheres.cod AND df_transportes.período = df_jovens_mulheres.período
        LEFT JOIN df_tx_transportes
        ON df_transportes.cod = df_tx_transportes.cod AND df_transportes.período = df_tx_transportes.período
        LEFT JOIN df_tx_mulheres
        ON df_transportes.cod = df_tx_mulheres.cod AND df_transportes.período = df_tx_mulheres.período
        LEFT JOIN df_tx_homens
        ON df_transportes.cod = df_tx_homens.cod AND df_transportes.período = df_tx_homens.período
        LEFT JOIN df_tx_jovens
        ON df_transportes.cod = df_tx_jovens.cod AND df_transportes.período = df_tx_jovens.período
        LEFT JOIN df_tx_jovens_homens
        ON df_transportes.cod = df_tx_jovens_homens.cod AND df_transportes.período = df_tx_jovens_homens.período
        LEFT JOIN df_tx_jovens_mulheres
        ON df_transportes.cod = df_tx_jovens_mulheres.cod AND df_transportes.período = df_tx_jovens_mulheres.período

        where df_transportes.período in ('2010','2011','2012','2013','2014','2015','2016','2017','2018','2019','2020')
    """

    # Using pandasql to execute query
    df_total = psql.sqldf(query, locals())

    # Cleaning unused dataframe
    del df_transportes
    del df_mulheres
    del df_homens
    del df_jovens
    del df_jovens_homens
    del df_jovens_mulheres
    del df_tx_transportes
    del df_tx_mulheres
    del df_tx_homens
    del df_tx_jovens
    del df_tx_jovens_homens
    del df_tx_jovens_mulheres
    
    # Garbage collect
    gc.collect()
    
    return df_total

def ler_acidentes(conn) -> None:
    """
    [Description]

        Reads SIMU files - Transport Accidents.
        Records with the fields necessary for the analysis.
    
    [Source]

        Link: https://bigdata-arquivos.icict.fiocruz.br/PUBLICO/SIMU/temas/simu-acidentes-transportes-mun-T.zip

    [Goal]

        Reading the .csv file through duckdb returning a dataframe.
    
    """
    x = conn.read_csv(f"""{RAW_PATH}SIMU - Acidentes de Transportes/Acidentes de Transportes.csv""")

    df_simu_acidentes = conn.execute("""SELECT * FROM x WHERE "Código IBGE" in ('3304557','5300108','3550308') """).df()

    # Rename column 'Código IBGE'
    df_simu_acidentes.rename(columns={'Código IBGE': 'cod'}, inplace=True)
    
    # Reading IPEA accidents
    df_ipea_acientes = ler_acidentes_IPEA(conn)

    # Rename column 'período'
    df_ipea_acientes.rename(columns={'período': 'ano'}, inplace=True)

    # Creating a new dataframe with merge between IPEA and SIMU data.
    df_result = df_simu_acidentes.merge(df_ipea_acientes, on=['cod','ano'], how='inner')

    # Creating .parquet file.
    df_result.to_parquet(f"""{SILVER_PATH}acidentes-geral.parquet""", engine='pyarrow', compression='snappy')
 
def ler_carteira(conn) -> None:
    """
    [Description]

        Reads the SIMU files - Enterprise Portfolio.
        Records with the fields necessary for the analysis.

    [Source]

        Link: https://bigdata-arquivos.icict.fiocruz.br/PUBLICO/SIMU/bases_dados/CARTEIRA/simu-carteira-mun-T.zip

    [Goal]

        Reading the .csv file through duckdb returning a dataframe.


    """
    x = conn.read_csv(f"""{RAW_PATH}simu-carteira-mun-T.csv""")

    result = conn.execute(
        f"""SELECT * FROM x WHERE "Código IBGE" in ({CODIGO_IBGE}) """
    ).df()

    # Create .parquet file.
    result.to_parquet(
        f"""{SILVER_PATH}simu-carteira-mun-T.parquet""", engine="pyarrow", compression="snappy"
    )

    # Cleaning dataframe.
    del result

    # Garbage collect.
    gc.collect()

def ler_frotas(conn) -> None:
    """
    [Description]

        Reads SIMU files - Motorization - Evolutionary Fleet.
        Records with the fields necessary for the analysis.

    [Source]

        Link: https://bigdata-arquivos.icict.fiocruz.br/PUBLICO/SIMU/bases_dados/FROTA/simu-frota-mun_T.zip

    [Goal]

        Reading the .csv file through duckdb returning a dataframe.

    """
    x = conn.read_csv(f"""{RAW_PATH}simu-frota-mun_T.csv""")

    result = conn.execute(
        f"""SELECT * FROM x WHERE "Código IBGE" in ({CODIGO_IBGE}) """
    ).df()

    # Creating .parquet file.
    result.to_parquet(
        f"""{SILVER_PATH}simu-frota-mun_T.parquet""", engine="pyarrow", compression="snappy"
    )
    # Cleaning dataframe.
    del result

    # Garbage collect.
    gc.collect()

def ler_etlsih_file(conn):
    """
    [Description]

        Reads files from the SUS Hospital Information System - SIHSUS.
        Records with the fields necessary for the analysis.
    
    [Source]

        Link: https://bigdata-arquivos.icict.fiocruz.br/PUBLICO/SIH/ETLSIH.zip

    [Goal]

        Reading of all files aggregated by UF, making them available in a .parquet file,
        seeking to obtain better performance in data analysis.
    
    """
    
    # Definição das variáveis de apoio.
    ano_inicial = ANO_INICIAL
    ano_final = ANO_FINAL
    serie = 12
    numero = 1
    ano = ano_inicial

    # Loop into brazillian states
    for estado in ESTADOS_BRASILEIROS:
        
        # Return initial variables to default value.
        ano = ano_inicial
        df_ano = None

        # Loop between years that will be analyze
        while ano_inicial <= ano_final:
            
            # Loop into months
            while numero <= serie:

                # Creating name of file to reading.
                nome_arquivo = f"""{RAW_PATH}ETLSIH/ETLSIH.ST_{estado}_{ano}_{numero}_t.csv"""

                try:
                    # Query to return fields that we need.
                    df = conn.execute(f"""SELECT int_muncod, 
                                                 int_munnome, 
                                                 ano_cmpt, 
                                                 mes_cmpt, 
                                                 def_sexo, 
                                                 val_sh, 
                                                 val_sp, 
                                                 val_tot, 
                                                 val_uti,
                                                 diag_princ, 
                                                 idade, 
                                                 raca_cor, 
                                                 int_capital, 
                                                 int_sigla_uf, 
                                                 int_codigo_uf, 
                                                 int_regiao, 
                                                 int_nome_uf,
                                                 dia_semana_internacao,
                                                 ano_internacao, 
                                                 mes_internacao, 
                                                 def_procedimento_realizado, 
                                                 def_procedimento_solicitado,
                                                 def_leitos, 
                                                 def_diag_princ_cap, 
                                                 def_diag_princ_grupo,
                                                 def_diag_secun_grupo,
                                                 def_diag_princ_cat,
                                                 def_diag_secun_cat,
                                                 def_diag_princ_subcat, 
                                                 def_car_int, 
                                                 def_cobranca,
                                                 def_morte, 
                                                 def_raca_cor, 
                                                 def_idade_pub,
                                                DIAGSEC1
                                      FROM read_csv('{nome_arquivo}') where (DIAGSEC1 LIKE 'V%' AND DIAGSEC1 NOT LIKE 'V9%') AND int_muncod IN ({CODIGO_IBGE_SIH});""").fetchdf()
                except FileNotFoundError as e:
                    print(f"Error: {e}")

                # Increment month.
                numero += 1

                # Concat first dataframe with dataframe in this loop
                df_ano = pd.concat([df_ano, df], ignore_index=True)
            
            # Transform ['MES_CMPT'] field to string.
            df_ano['MES_CMPT'] = df_ano['MES_CMPT'].astype(str)
            
            # Increment month and year variables.
            ano = ano + 1
            ano_inicial = ano
            numero = 1

        # Creating .parquet file agregate all years per state
        df_ano.to_parquet(f"""{SILVER_PATH}ETLSIH_parquet/ETLSIH.ST_{estado}.parquet""", engine='pyarrow', compression='snappy')

        # Increment support variables.
        numero = 1
        ano_inicial = ANO_INICIAL

def analise(conn) -> None:
    """
    [Description]

        Performing data analysis.
    
    [Source]

        None

    [Goal]

        Reading and join information from all .parquet files, to analyze all of data.
    
    """

    # Performing .parquet files in dataframe.

    acidentes = conn.read_parquet("acidentes-geral.parquet").df()

    carteira = conn.read_parquet("simu-carteira-mun-T.parquet").df()

    frota = conn.read_parquet("simu-frota-mun_T.parquet").df()

    sih = conn.read_parquet("ETLSIH_parquet/ETLSIH.ST_*.parquet", union_by_name=True).df()
    
    # Convert [cod] field to string.
    acidentes['cod_sih'] = acidentes['cod'].astype(str)

    # Removing last [cod] character to use as key in merge with ETLSIH data.
    acidentes['cod_sih'] = acidentes['cod_sih'].str[:-1]  

    # Convert [Código IBGE] field to string.
    carteira['cod_sih'] = carteira['Código IBGE'].astype(str)

    # Removing last [cod] character to use as key in merge with ETLSIH data.
    carteira['cod_sih'] = carteira['cod_sih'].str[:-3]

    # Convert [Código IBGE] field to string.
    frota['cod_sih'] = frota['Código IBGE'].astype(str)

    # Removing last [cod] character to use as key in merge with ETLSIH data.
    frota['cod_sih'] = frota['cod_sih'].str[:-1]

    # Rename [ANO_CMPT] field to use as key in merge.
    sih.rename(columns={"ANO_CMPT": "ano"}, inplace=True)

    # Rename [int_MUNCOD] field to use as key in merge.
    sih.rename(columns={"int_MUNCOD": "cod"}, inplace=True)

    # Convert [cod] field to string.
    sih['cod_sih'] = sih['cod'].astype(str)

    """

    Data analyze from SIMU files - Motorization - Evolutionary Fleet vs data from ETLSIH.
    
    Study of the increase in rates and hospitalizations caused by traffic accidents for municipalities of Rio de Janeiro, São Paulo and the Federal District by population size.

    """

    # Performing merge with ETLSIH and Evolutionary Fleet
    df_sih_frota = sih.merge(
        frota, on=["cod_sih", "ano"], how="inner"
    )

    # Select columns to use.
    df_sih_frota = df_sih_frota[['int_MUNNOME', 'ano','TOTAL_VEICULOS','Populacao']]

    # Counts hospitalizations grouped by year and municipality.
    df_sih_frota = df_sih_frota.groupby(['int_MUNNOME', 'ano','TOTAL_VEICULOS','Populacao']).size().reset_index(name='TOTAL_INTERNACOES')

    # Calculates the rate of hospitalizations per population per year.
    df_sih_frota['TAXA_INTERNACOES_POPULACAO'] = df_sih_frota['TOTAL_INTERNACOES']/df_sih_frota['Populacao']*100

    # Calculates the rate of population increase each year by municipality.
    df_sih_frota['PERCENTUAL_AUMENTO_POPULACAO'] = df_sih_frota.groupby('int_MUNNOME')['Populacao'].pct_change()

    print("Analysis of ETLSIH data (Hospital Data) x Fleet and Population")
    print(df_sih_frota)

    """

    Analysis of accident data from IPEA and SIMU x Frota

    Study of the increase in deaths/injuries per number of vehicles/population and accident rate per population for the municipalities of Rio de Janeiro, São Paulo and the Federal District.

    """

    # Merges IPEA and SIMU data with Fleet data
    df_acidentes_frota = acidentes.merge(
        frota, on=["cod_sih", "ano", "Município"], how="inner"
    )

    # Defines the columns to be analyzed.
    df_acidentes_frota = df_acidentes_frota[['cod_sih','Município', 'ano','TOTAL_VEICULOS','Populacao_y', 'total_mortes', 'total_feridos']]

    # Calculates the total number of accidents.
    df_acidentes_frota['TOTAL_DE_ACIDENTES'] = df_acidentes_frota['total_mortes']+df_acidentes_frota['total_feridos']

    # Calculates the rate of Deaths due to Injuries.
    df_acidentes_frota['TAXA_FERIDOS_MORTES'] = df_acidentes_frota['total_mortes']/df_acidentes_frota['total_feridos']*100

    # Calculates the population rate per vehicle.
    df_acidentes_frota['TAXA_POPULACAO_VEICULOS'] = df_acidentes_frota['TOTAL_VEICULOS']/df_acidentes_frota['Populacao_y']*100

    # Calculates the Accident rate by Population.
    df_acidentes_frota['TAXA_ACIDENTES_POPULACAO'] = (df_acidentes_frota['total_mortes'] + df_acidentes_frota['total_feridos'])/df_acidentes_frota['Populacao_y']*100

    # Calculates the rate of population increase each year by municipality.
    df_acidentes_frota['PERCENTUAL_AUMENTO_ACIDENTES'] = df_acidentes_frota.groupby('Município')['TOTAL_DE_ACIDENTES'].pct_change()

    print("Analysis of accident data from IPEA and SIMU x Frota")
    print(df_acidentes_frota)

    """

    Analysis of ETLSIH data (Hospital Data) x Enterprise Portfolio

    Study of the increase in hospitalizations for the municipalities of Rio de Janeiro, São Paulo and the Federal District associated with the portfolio of projects.

    """

    # Counts the works carried out grouping them by year and municipality.
    carteira_valores = carteira.groupby(['cod_sih', 'ano_fim_obra']).size().reset_index(name='TOTAL_OBRAS')

    # Format the [year] field
    carteira_valores['ano'] = carteira_valores['ano_fim_obra'].astype(str).str[:-2]

    # Remove unnecessary field
    carteira_valores = carteira_valores.drop(columns=['ano_fim_obra'])

    # Realiza a contagem das internações agrupando por ano e município.
    sih_valores = sih.groupby(['cod_sih', 'ano', 'int_MUNNOME']).size().reset_index(name='TOTAL_INTERNACOES')

    # Counts hospitalizations grouped by year and municipality.
    sih_valores['ano'] = sih_valores['ano'].astype(str)

    # Merge SIH data with Portfolio data
    df_sih_carteira = sih_valores.merge(
        carteira_valores, on=["cod_sih", "ano"], how="inner"
    )

    # Calculates the rate of increase in hospitalizations each year by municipality and number of works
    df_sih_carteira['PERCENTUAL_AUMENTO_INTERNACOES'] = df_sih_carteira.groupby('int_MUNNOME')['TOTAL_INTERNACOES'].pct_change()

    print("Analysis of ETLSIH data (Hospital Data) x Enterprise Portfolio")
    print(df_sih_carteira)

if __name__ == '__main__':

    conn = duckdb.connect(':memory:')

    print(ler_acidentes_IPEA(conn))

    ler_acidentes(conn)

    ler_carteira(conn)

    ler_frotas(conn)

    analise(conn)

    