import pandas as pd
import pandas.io.sql
import pyodbc
import numpy as np

"""
    Neste arquivo, estão as funções para cálculo do Coeficiente de Rendimento (CR)
    
    A fórmula para cálculo do CR é:
        
        CR = (Mp + Mg) / 20
        
        Onde:
        
        CR = Coeficiente de Rendimento
        Mp = Média primária. Esta média considera apenas as primeiras provas realizadas pelo usuário no portal
        Mg = Média geral. Esta média considera todas as provas realizadas pelo usuário no portal
        
        **** A divisão da média é por 20 para já a adequar à escala de probabilidade (0 a 1 ou de 0% a 100%)
    
    O CR permite que se saiba o quão bem o usuário foi no geral ou em certa categoria ou treinamento, ponderando seu 
    desempenho inicial e geral. O índice do CR vai de 0 a 1 (0% a 100%) e quanto mais alto, melhor é a avaliação.
       
    As consultas são feitas usando as variáveis globais abaixo, que servem para se conectar ao banco e fazer a busca.
    As funções permitem que se utilize filtros opcionais para se refinar a busca.
    
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

# Calcula o CR de um usuário fazendo a busca via FUNCIONAL. Categoria e Treinamento são filtros de CR opcionais
def por_funcional(funcional, categoria=None, treinamento=None):

    try:
        if categoria is not None:
            if treinamento is not None:

                # Filtro por categoria e treinamento
                resultado = pd.read_sql_query(_query + "\nWHERE [funcional] = "
                                              + funcional
                                              + " AND [categoria] = '"
                                              + categoria + "'"
                                              + " AND [treinamento] = '"
                                              + treinamento + "'", _conexao)
                coeficiente = (resultado['nota'].mean() + resultado.groupby('treinamento').first()['nota'].mean()) / 20

            else:

                # Filtro por categoria
                resultado = pd.read_sql_query(_query + "\nWHERE [funcional] = "
                                              + funcional
                                              + " AND [categoria] = '"
                                              + categoria + "'", _conexao)
                coeficiente = (resultado['nota'].mean() + resultado.groupby('treinamento').first()['nota'].mean()) / 20

        elif treinamento is not None:

            # Filtro por treinamento
            resultado = pd.read_sql_query(_query + "\nWHERE [funcional] = "
                                          + funcional
                                          + " AND [treinamento] = '"
                                          + treinamento + "'", _conexao)
            coeficiente = (resultado['nota'].mean() + resultado.groupby('treinamento').first()['nota'].mean()) / 20

        else:

            # Sem filtos
            resultado = pd.read_sql_query(_query + "\nWHERE [funcional] = " + funcional, _conexao)
            coeficiente = (resultado['nota'].mean() + resultado.groupby('treinamento').first()['nota'].mean()) / 20

    # Erro na query de seleção
    except pd.io.sql.DatabaseError:
        return None

    return coeficiente

# Calcula o CR de um usuário fazendo a busca via RACF. Categoria e Treinamento são filtros de CR opcionais
def por_racf(racf, categoria=None, treinamento=None):

    try:
        if categoria is not None:
            if treinamento is not None:

                # Filtro por categoria e treinamento
                resultado = pd.read_sql_query(_query + "\nWHERE [racf] = '"
                                              + racf + "'"
                                              + " AND [categoria] = '"
                                              + categoria + "'"
                                              + " AND [treinamento] = '"
                                              + treinamento + "'", _conexao)
                coeficiente = (resultado['nota'].mean() + resultado.groupby('treinamento').first()['nota'].mean()) / 20

            else:

                # Filtro por categoria
                resultado = pd.read_sql_query(_query + "\nWHERE [racf] = '"
                                              + racf + "'"
                                              + " AND [categoria] = '"
                                              + categoria + "'", _conexao)
                coeficiente = (resultado['nota'].mean() + resultado.groupby('treinamento').first()['nota'].mean()) / 20

        elif treinamento is not None:

            # Filtro por treinamento
            resultado = pd.read_sql_query(_query + "\nWHERE [racf] = '"
                                          + racf + "'"
                                          + " AND [treinamento] = '"
                                          + treinamento + "'", _conexao)
            coeficiente = (resultado['nota'].mean() + resultado.groupby('treinamento').first()['nota'].mean()) / 20

        else:

            # Sem filtros
            resultado = pd.read_sql_query(_query + "\nWHERE [racf] = '" + racf + "'", _conexao)
            coeficiente = (resultado['nota'].mean() + resultado.groupby('treinamento').first()['nota'].mean()) / 20

    # Erro na query de seleção
    except pd.io.sql.DatabaseError:
        return None

    return coeficiente

# Calcula o CR de um usuário fazendo a busca via NOME. Categoria e Treinamento são filtros de CR opcionais
def por_nome(nome, categoria=None, treinamento=None):

    try:
        if categoria is not None:
            if treinamento is not None:

                # Filtro por categoria e treinamento
                resultado = pd.read_sql_query(_query + "\nWHERE [nome] = '"
                                              + nome + "'"
                                              + " AND [categoria] = '"
                                              + categoria + "'"
                                              + " AND [treinamento] = '"
                                              + treinamento + "'", _conexao)
                coeficiente = (resultado['nota'].mean() + resultado.groupby('treinamento').first()['nota'].mean()) / 20

            else:

                # Filtro por categoria
                resultado = pd.read_sql_query(_query + "\nWHERE [nome] = '"
                                              + nome + "'"
                                              + " AND [categoria] = '"
                                              + categoria + "'", _conexao)
                coeficiente = (resultado['nota'].mean() + resultado.groupby('treinamento').first()['nota'].mean()) / 20

        elif treinamento is not None:

            # Filtro por treinamento
            resultado = pd.read_sql_query(_query + "\nWHERE [nome] = '"
                                          + nome + "'"
                                          + " AND [treinamento] = '"
                                          + treinamento + "'", _conexao)
            coeficiente = (resultado['nota'].mean() + resultado.groupby('treinamento').first()['nota'].mean()) / 20

        else:

            # Sem filtros
            resultado = pd.read_sql_query(_query + "\nWHERE [nome] = '" + nome + "'", _conexao)
            coeficiente = (resultado['nota'].mean() + resultado.groupby('treinamento').first()['nota'].mean()) / 20

    # Erro na query de seleção
    except pd.io.sql.DatabaseError:
        return None

    return coeficiente


'''
  Calcula todos os CRs de todos os usuários que realizaram ao menos uma prova e retorna uma lista ranqueada.
  Categoria é um filtro opcional e top20 retorna apenas os 20 primeiros colocados. Em caso de empate, o primeiro a fazer
as provas fica na frente.
  Por padrão, retorna nome e funcional. Mas a função também permite se escolher o que a tabela retornará.
'''
def ranking_coeficentes(categoria=None, nome=True, funcional=False, top20=False):

    try:

        temp = []
        ranking = pd.DataFrame()

        # Filtro categoria
        if categoria is not None:

            lista_nomes = pd.read_sql_query(_query + "\nWHERE [categoria] = '" + categoria + "'", _conexao)
            lista_nomes = lista_nomes.drop_duplicates(subset=['nome', 'funcional'])

            for index, row in lista_nomes.iterrows():
                if funcional and nome:
                    temp.append([row['nome'], row['funcional'], por_nome(row['nome'], categoria=categoria)])
                elif funcional and not nome:
                    temp.append([row['funcional'], por_nome(row['nome'], categoria=categoria)])
                elif not funcional and nome:
                    temp.append([row['nome'], por_nome(row['nome'], categoria=categoria)])
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

            lista_nomes = pd.read_sql_query(_query, _conexao)
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

# Calcula o CR total geral de todos os usuários que fizeram prova. Pode-se aplicar filtros de categoria e/ou treinamento
def rendimento_medio(categoria=None, treinamento=None):

    try:

        temp = []

        if categoria is not None:
            if treinamento is not None:

                # Filtro por categoria e treinamento
                resultado = pd.read_sql_query(_query + "\nWHERE [categoria] = '" + categoria + "'"
                                              + " AND [treinamento] = '" + treinamento + "'", _conexao)
                lista_nomes = resultado['nome'].unique()

                for nome in lista_nomes:
                    cr_pessoa = por_nome(nome, categoria=categoria, treinamento=treinamento)
                    temp.append(cr_pessoa)

                coefiente = np.array(temp).mean()

            else:

                # Filtro por categoria
                resultado = pd.read_sql_query(_query + "\nWHERE [categoria] = '" + categoria + "'", _conexao)
                lista_nomes = resultado['nome'].unique()

                for nome in lista_nomes:
                    cr_pessoa = por_nome(nome, categoria=categoria)
                    temp.append(cr_pessoa)

                coefiente = np.array(temp).mean()

        elif treinamento is not None:

            # Filtro por treinamento
            resultado = pd.read_sql_query(_query + "\nWHERE [treinamento] = '" + treinamento + "'", _conexao)
            lista_nomes = resultado['nome'].unique()

            for nome in lista_nomes:
                cr_pessoa = por_nome(nome, treinamento=treinamento)
                temp.append(cr_pessoa)

            coefiente = np.array(temp).mean()

        else:

            # Sem filtros
            resultado = pd.read_sql_query(_query, _conexao)

            lista_nomes = resultado['nome'].unique()

            for nome in lista_nomes:
                cr_pessoa = por_nome(nome)
                temp.append(cr_pessoa)

            coefiente = np.array(temp).mean()

    # Erro na query de seleção
    except pd.io.sql.DatabaseError:
        return None

    return coefiente
