import pandas as pd
import pandas.io.sql
import pyodbc

"""
    Neste arquivo, estão as funções para cálculo de Médias

    As funções implementadas abaixo permitem três formas distintas de cálculo de médias, são elas:

        - Média Geral (MG): média aritmética de todas as provas realizadas. Fórmula:
            
            MG = ( Σ (PR1 + ... PRn) ) / n
            
            Sendo:
            
            MG = Média Geral
            PR = Provas Realizadas
            n  = Número de provas realizadas
            
            **** Σ reprsenta o somatório
            
        - Média Primária (MP): média artmética de todas as primeiras provas realizadas. Fórmula:
         
            MP = ( Σ (PP1 + ... PPn) ) / n
            
            Sendo:
            
            MG = Média Primária
            PP = Primeira prova. Corresponde a primeira prova realizada de um treinamento
            n  = Número de treinamentos com prova realizadas. Difere do acima porque considera apenas uma prova por
                 treinamento
            
            **** Σ reprsenta o somatório
            
        - Média Final (MP): média artmética de todas as maiores notas por prova realizada. É a média mostrada no portal.
        Fórmula:
         
            MF = ( Σ (MNT1 + ... MNTn) ) / n
            
            Sendo:
            
            MG  = Média Final
            MNT = Maior nota do Treinamento. Considera apenas a maior nota por treinamento.
            n   = Número de treinamentos com prova realizadas. Difere do acima porque considera apenas uma prova por
                  treinamento
            
            ***** Σ reprsenta o somatório
            
    Cada média tem seu uso distinto e traz uma informação diferente. MG traz todo o histórico do usuário no IU Mentoria.
    MP mostra as primeiras impressões de um usuário. E estas duas médias compõem o CR, presente no arquivo de rendimento.
    Já MF é a média com os melhores resultados do usuário. Mostra todo o resultado dos estudos do desenvolvimento do 
    usuário.

    As consultas são feitas usando as variáveis globais abaixo, que servem para se conectar ao banco e fazer as buscas.
    As funções permitem que se utilize filtros opcionais para se refinar a busca. As médias também são escolhidas em uma
    variável de opção.

"""

"""VARIÁVEIS GLOBAIS"""
_conexao = pyodbc.connect(r'Driver={ODBC Driver 17 for SQL Server};Server=10.58.56.161;Database=mentoria;Trusted_Connection=yes;Encrypt=no')
_query = """SELECT
	[nome],
	[racf],
	[funcional],
	[treinamento],
	[nota],
	[categoria]
    FROM
	[dbo].[coeficiente_mentoria]"""

"""FUNÇÕES"""

# Calcula média via FUNCIONAL. Categoria e Treinamento são filtros opcionais. Opção permite escolher o tipo de média
def por_funcional(funcional, categoria=None, treinamento=None, opcao=None):

    # Opções disponíveis
    opcoes = ['geral', 'primaria', 'final', 'todas', None]

    # Caso a opção seja diferente das possíveis
    if opcao not in opcoes:
        return 'Opção de média inválida'

    try:
        if categoria is not None:

            # Filtro por treinamento e categoria
            if treinamento is not None:

                resultado = pd.read_sql_query(_query + "\nWHERE [funcional] = "
                                              + funcional
                                              + " AND [categoria] = '"
                                              + categoria + "'"
                                              + " AND [treinamento] = '"
                                              + treinamento + "'", _conexao)

            # Filtro por categoria
            else:

                resultado = pd.read_sql_query(_query + "\nWHERE [funcional] = "
                                              + funcional
                                              + " AND [categoria] = '"
                                              + categoria + "'", _conexao)

        # Filtro por treinamento
        elif treinamento is not None:

            resultado = pd.read_sql_query(_query + "\nWHERE [funcional] = "
                                          + funcional
                                          + " AND [treinamento] = '"
                                          + treinamento + "'", _conexao)

        # Sem filtro
        else:
            resultado = pd.read_sql_query(_query + "\nWHERE [funcional] = " + funcional, _conexao)

    # Erro na query de seleção
    except pd.io.sql.DatabaseError:
        return None

    # Opções de média

    # Geral
    if opcao == 'geral':
        media = resultado['nota'].mean()

    # Primaria
    elif opcao == 'primaria':
        media = resultado.groupby('treinamento').first()['nota'].mean()

    # Final
    elif opcao == 'final':
        media = resultado.groupby('treinamento')['nota'].max().mean()

    # Todas. Retorna uma tupla com os três tipos de média
    elif opcao == 'todas':
        media = {'geral': resultado['nota'].mean(),
                 'primaria': resultado.groupby('treinamento').first()['nota'].mean(),
                 'final': resultado.groupby('treinamento')['nota'].max().mean()}

    # Sem opções selecionadas. Por padrão a média é a geral
    else:
        media = resultado['nota'].mean()

    return media

# Calcula média via RACF. Categoria e Treinamento são filtros opcionais. Opção permite escolher o tipo de média
def por_racf(racf, categoria=None, treinamento=None, opcao=None):

    # Opções disponíveis
    opcoes = ['geral', 'primaria', 'final', 'todas', None]

    # Caso a opção seja diferente das possíveis
    if opcao not in opcoes:
        return 'Opção de média inválida'

    try:
        if categoria is not None:

            # Filtro por treinamento e categoria
            if treinamento is not None:

                resultado = pd.read_sql_query(_query + "\nWHERE [racf] = '"
                                              + racf + "'"
                                              + " AND [categoria] = '"
                                              + categoria + "'"
                                              + " AND [treinamento] = '"
                                              + treinamento + "'", _conexao)

            # Filtro por categoria
            else:

                resultado = pd.read_sql_query(_query + "\nWHERE [racf] = '"
                                              + racf + "'"
                                              + " AND [categoria] = '"
                                              + categoria + "'", _conexao)

        # Filtro por treinamento
        elif treinamento is not None:

            resultado = pd.read_sql_query(_query + "\nWHERE [racf] = '"
                                          + racf + "'"
                                          + " AND [treinamento] = '"
                                          + treinamento + "'", _conexao)

        # Sem filtro
        else:
            resultado = pd.read_sql_query(_query + "\nWHERE [racf] = '" + racf + "'", _conexao)

    # Erro na query de seleção
    except pd.io.sql.DatabaseError:
        return None

    # Opções de média

    # Geral
    if opcao == 'geral':
        media = resultado['nota'].mean()

    # Primaria
    elif opcao == 'primaria':
        media = resultado.groupby('treinamento').first()['nota'].mean()

    # Final
    elif opcao == 'final':
        media = resultado.groupby('treinamento')['nota'].max().mean()

    # Todas. Retorna uma tupla com os três tipos de média
    elif opcao == 'todas':
        media = {'geral': resultado['nota'].mean(),
                 'primaria': resultado.groupby('treinamento').first()['nota'].mean(),
                 'final': resultado.groupby('treinamento')['nota'].max().mean()}

    # Sem opções selecionadas. Por padrão a média é a geral
    else:
        media = resultado['nota'].mean()

    return media

# Calcula média via NOME. Categoria e Treinamento são filtros opcionais. Opção permite escolher o tipo de média
def por_nome(nome, categoria=None, treinamento=None, opcao=None):

    # Opções disponíveis
    opcoes = ['geral', 'primaria', 'final', 'todas', None]

    # Caso a opção seja diferente das possíveis
    if opcao not in opcoes:
        return 'Opção de média inválida'

    try:
        if categoria is not None:

            # Filtro por treinamento e categoria
            if treinamento is not None:

                resultado = pd.read_sql_query(_query + "\nWHERE [nome] = '"
                                              + nome + "'"
                                              + " AND [categoria] = '"
                                              + categoria + "'"
                                              + " AND [treinamento] = '"
                                              + treinamento + "'", _conexao)

            # Filtro por categoria
            else:

                resultado = pd.read_sql_query(_query + "\nWHERE [nome] = '"
                                              + nome + "'"
                                              + " AND [categoria] = '"
                                              + categoria + "'", _conexao)

        # Filtro por treinamento
        elif treinamento is not None:

            resultado = pd.read_sql_query(_query + "\nWHERE [nome] = '"
                                          + nome + "'"
                                          + " AND [treinamento] = '"
                                          + treinamento + "'", _conexao)

        # Sem filtro
        else:
            resultado = pd.read_sql_query(_query + "\nWHERE [nome] = '" + nome + "'", _conexao)

    # Erro na query de seleção
    except pd.io.sql.DatabaseError:
        return None

    # Opções de média

    # Geral
    if opcao == 'geral':
        media = resultado['nota'].mean()

    # Primaria
    elif opcao == 'primaria':
        media = resultado.groupby('treinamento').first()['nota'].mean()

    # Final
    elif opcao == 'final':
        media = resultado.groupby('treinamento')['nota'].max().mean()

    # Todas. Retorna uma tupla com os três tipos de média
    elif opcao == 'todas':
        media = {'geral': resultado['nota'].mean(),
                 'primaria': resultado.groupby('treinamento').first()['nota'].mean(),
                 'final': resultado.groupby('treinamento')['nota'].max().mean()}

    # Sem opções selecionadas. Por padrão a média é a geral
    else:
        media = resultado['nota'].mean()

    return media

# Calcula média de notas por CATEGORIA. Opção permite escolher o tipo de média
def por_categoria(categoria, opcao=None):

    # Opções disponíveis
    opcoes = ['geral', 'primaria', 'final', 'todas', None]

    # Caso a opção seja diferente das possíveis
    if opcao not in opcoes:
        return 'Opção de média inválida'

    try:
        resultado = pd.read_sql_query(_query + "\nWHERE [categoria] = '" + categoria + "'", _conexao)

    # Erro na query de seleção
    except pd.io.sql.DatabaseError:
        return None

    # Opções de média

    # Geral
    if opcao == 'geral':
        media = resultado['nota'].mean()

    # Primaria
    elif opcao == 'primaria':
        media = resultado.groupby('nome').first()['nota'].mean()

    # Final
    elif opcao == 'final':
        media = resultado.groupby('nome')['nota'].max().mean()

    # Todas. Retorna uma tupla com os três tipos de média
    elif opcao == 'todas':
        media = {'geral': resultado['nota'].mean(),
                 'primaria': resultado.groupby('treinamento').first()['nota'].mean(),
                 'final': resultado.groupby('treinamento')['nota'].max().mean()}

    # Sem opções selecionadas. Por padrão a média é a geral
    else:
        media = resultado['nota'].mean()

    return media

# Calcula média de notas por TREINAMENTO. Opção permite escolher o tipo de média
def por_treinamento(treinamento, opcao=None):

    # Opções disponíveis
    opcoes = ['geral', 'primaria', 'final', 'todas', None]

    # Caso a opção seja diferente das possíveis
    if opcao not in opcoes:
        return 'Opção de média inválida'

    try:
        resultado = pd.read_sql_query(_query + "\nWHERE [treinamento] = '" + treinamento + "'", _conexao)

    # Erro na query de seleção
    except pd.io.sql.DatabaseError:
        return None

    # Opções de média

    # Geral
    if opcao == 'geral':
        media = resultado['nota'].mean()

    # Primaria
    elif opcao == 'primaria':
        media = resultado.groupby('nome').first()['nota'].mean()

    # Final
    elif opcao == 'final':
        media = resultado.groupby('nome')['nota'].max().mean()

    # Todas. Retorna uma tupla com os três tipos de média
    elif opcao == 'todas':
        media = {'geral': resultado['nota'].mean(),
                 'primaria': resultado.groupby('treinamento').first()['nota'].mean(),
                 'final': resultado.groupby('treinamento')['nota'].max().mean()}

    # Sem opções selecionadas. Por padrão a média é a geral
    else:
        media = resultado['nota'].mean()

    return media
