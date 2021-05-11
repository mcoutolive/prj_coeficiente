import pandas as pd
import numpy as np
import pandas.io.sql
import pyodbc

"""
    Neste arquivo, estão as funções para cálculo do Coeficiente de Progressão (CP)

    A fórmula para cálculo do CP é:

        CP = TPUr / TPd

        Onde:

        CP   = Coeficiente de Progressão
        TPUr = Total de Provas Ùnicas realizadas. Conta apenas uma prova por treinamento feita pelo usuário
        TPd  = Total de Provas Disponíveis. Conta todos os cursos que possuem prova

    O CP serve para mostrar quanto que o usuário avançou numa trilha ds conhecimento ou na plataforma como um todo,
    baseando-se nas provas realizadas e disponíveis. O índice varia de 0 a 1 (0% a 100%) e quanto mais próximo de 1,
    mais da trilha foi concluída. 1 indica que toda a trilha foi concluída e 0 que nada foi.

    As consultas são feitas usando as variáveis globais abaixo, que servem para se conectar ao banco e fazer as buscas.
    As funções permitem que se utilize filtros opcionais para se refinar a busca.

"""

"""VARIÁVEIS GLOBAIS"""
_conexao = pyodbc.connect(r'Driver={ODBC Driver 17 for SQL Server};Server=10.58.56.161;Database=mentoria;Trusted_Connection=yes;Encrypt=no')
_query_pessoa = """SELECT
	[nome],
	[racf],
	[funcional],
	[treinamento],
	[nota],
	[categoria]
    FROM
	[dbo].[coeficiente_mentoria]"""
_query_treinamentos = """SELECT
	Treinamento.[titulo] AS 'Treinamento',
	Categoria.[descricao] AS 'Categoria'
    FROM
	(([mentoria].[dbo].[treinamento] AS Treinamento
	INNER JOIN [mentoria].[dbo].[prova] AS Prova ON Treinamento.[id] = Prova.[treinamento_id])
	INNER JOIN [mentoria].[dbo].[categoria] AS Categoria ON Treinamento.[categoria_id] = Categoria.[id])"""

"""FUNÇÕES"""

# Calcula o CP de um usuário fazendo a busca via FUNCIONAL. Categoria é um filtro opcional
def por_funcional(funcional, categoria=None):

    try:

        # Filtro por categoria
        if categoria is not None:

            total_provas = pd.read_sql_query(_query_treinamentos
                                             + " WHERE Categoria.[descricao] = '"
                                             + categoria + "'", _conexao)
            total_pessoa = pd.read_sql_query(_query_pessoa
                                             + "WHERE [funcional] = '"
                                             + funcional + "'"
                                             + "AND [categoria] = '"
                                             + categoria + "'", _conexao)

            coeficiente = total_pessoa['treinamento'].nunique() / total_provas['Treinamento'].count()

        # Sem filtro
        else:

            total_provas = pd.read_sql_query(_query_treinamentos, _conexao)
            total_pessoa = pd.read_sql_query(_query_pessoa
                                             + "WHERE [funcional] = '"
                                             + funcional + "'", _conexao)

            coeficiente = total_pessoa['treinamento'].nunique() / total_provas['Treinamento'].count()

    # Erro na query de seleção
    except pandas.io.sql.DatabaseError:
        return None

    return coeficiente

# Calcula o CP de um usuário fazendo a busca via FUNCIONAL. Categoria é um filtro opcional
def por_racf(racf, categoria=None):
    try:

        # Filtro por categoria
        if categoria is not None:

            total_provas = pd.read_sql_query(_query_treinamentos
                                             + " WHERE Categoria.[descricao] = '"
                                             + categoria + "'", _conexao)
            total_pessoa = pd.read_sql_query(_query_pessoa
                                             + "WHERE [racf] = '"
                                             + racf + "'"
                                             + "AND [categoria] = '"
                                             + categoria + "'", _conexao)

            coeficiente = total_pessoa['treinamento'].nunique() / total_provas['Treinamento'].count()

        # Sem filtro
        else:

            total_provas = pd.read_sql_query(_query_treinamentos, _conexao)
            total_pessoa = pd.read_sql_query(_query_pessoa
                                             + "WHERE [racf] = '"
                                             + racf + "'", _conexao)

            coeficiente = total_pessoa['treinamento'].nunique() / total_provas['Treinamento'].count()

    # Erro na query de seleção
    except pandas.io.sql.DatabaseError:
        return None

    return coeficiente

# Calcula o CP de um usuário fazendo a busca via FUNCIONAL. Categoria é um filtro opcional
def por_nome(nome, categoria=None):
    try:

        # Filtro por categoria
        if categoria is not None:

            total_provas = pd.read_sql_query(_query_treinamentos
                                             + " WHERE Categoria.[descricao] = '"
                                             + categoria + "'", _conexao)
            total_pessoa = pd.read_sql_query(_query_pessoa
                                             + "WHERE [nome] = '"
                                             + nome + "'"
                                             + "AND [categoria] = '"
                                             + categoria + "'", _conexao)

            coeficiente = total_pessoa['treinamento'].nunique() / total_provas['Treinamento'].count()

        # Sem filtro
        else:

            total_provas = pd.read_sql_query(_query_treinamentos, _conexao)
            total_pessoa = pd.read_sql_query(_query_pessoa
                                             + "WHERE [nome] = '"
                                             + nome + "'", _conexao)

            coeficiente = total_pessoa['treinamento'].nunique() / total_provas['Treinamento'].count()

    # Erro na query de seleção
    except pandas.io.sql.DatabaseError:
        return None

    return coeficiente

# Calcula a média de CP de todos os usuários que fizeram provas. Categoria é um filtro opcional
def progressao_media(categoria=None):

    try:

        temp = []

        # Filtro por categoria
        if categoria is not None:

            lista_nomes = pd.read_sql_query(_query_pessoa + "WHERE [categoria] = '" + categoria + "'", _conexao)
            lista_nomes = lista_nomes['nome'].unique()

            for nome in lista_nomes:
                coeficiente_pessoa = por_nome(nome, categoria=categoria)
                temp.append(coeficiente_pessoa)

            coeficiente = np.array(temp).mean()

        # Sem filtro
        else:

            consulta_nomes = pd.read_sql_query(_query_pessoa, _conexao)
            lista_nomes = consulta_nomes['nome'].unique()

            for nome in lista_nomes:
                coeficiente_pessoa = por_nome(nome)
                temp.append(coeficiente_pessoa)

            coeficiente = np.array(temp).mean()

    # Erro na query de seleção
    except pandas.io.sql.DatabaseError:
        return None

    return coeficiente

'''
  Calcula todos os CRs de todos os usuários que realizaram ao menos uma prova e retorna uma lista ranqueada.
  Categoria é um filtro opcional e top20 retorna apenas os 20 primeiros colocados. Em caso de empate, o primeiro a fazer
as provas fica na frente.
  Por padrão, retorna nome e funcional. Mas a função também permite se escolher o que a tabela retornará.
'''
def ranking_progressao(categoria=None, nome=True, funcional=False, top20=False):

    try:

        temp = []
        ranking = pd.DataFrame()

        # Filtro categoria
        if categoria is not None:

            lista_nomes = pd.read_sql_query(_query_pessoa, _conexao)
            lista_nomes = lista_nomes.drop_duplicates(subset=['nome', 'funcional'])

            for index, row in lista_nomes.iterrows():
                if funcional and nome:
                    temp.append([row['nome'], row['funcional'], por_nome(row['nome'])])
                elif funcional and not nome:
                    temp.append([row['funcional'], por_nome(row['nome'])])
                elif not funcional and nome:
                    temp.append([row['nome'], por_nome(row['nome'])])
                else:
                    temp.append(por_nome(row['nome']))

            if funcional and nome:
                ranking = ranking.append(pd.DataFrame(temp, columns=['Nome', 'Funcional', 'Coeficiente']))
            elif funcional and not nome:
                ranking = ranking.append(pd.DataFrame(temp, columns=['Funcional', 'Coeficiente']))
            elif not funcional and nome:
                ranking = ranking.append(pd.DataFrame(temp, columns=['Nome', 'Coeficiente']))
            else:
                ranking = ranking.append(pd.DataFrame(temp, columns=['Coeficiente']))

        # Sem filtos
        else:

            lista_nomes = pd.read_sql_query(_query_pessoa, _conexao)
            lista_nomes = lista_nomes.drop_duplicates(subset=['nome', 'funcional'])

            for index, row in lista_nomes.iterrows():
                if funcional and nome:
                    temp.append([row['nome'], row['funcional'], por_nome(row['nome'])])
                elif funcional and not nome:
                    temp.append([row['funcional'], por_nome(row['nome'])])
                elif not funcional and nome:
                    temp.append([row['nome'], por_nome(row['nome'])])
                else:
                    temp.append(por_nome(row['nome']))

            if funcional and nome:
                ranking = ranking.append(pd.DataFrame(temp, columns=['Nome', 'Funcional', 'Coeficiente']))
            elif funcional and not nome:
                ranking = ranking.append(pd.DataFrame(temp, columns=['Funcional', 'Coeficiente']))
            elif not funcional and nome:
                ranking = ranking.append(pd.DataFrame(temp, columns=['Nome', 'Coeficiente']))
            else:
                ranking = ranking.append(pd.DataFrame(temp, columns=['Coeficiente']))

        ranking = ranking.rename_axis('ID').sort_values(by=['Coeficiente', 'ID'], ascending=[False, True])

        # Apenas os 20 primeiros
        if top20:
            ranking = ranking.head(20)


    # Erro nas queries de seleção
    except pd.io.sql.DatabaseError:
        return None

    # Ranking vazio
    if ranking.empty:
        return None

    return ranking
