import pandas as pd
import pandas.io.sql
import pyodbc

"""
    Neste arquivo, estão as funções para cálculo do Score Final (SF). Este score se resulta de consultas dos três
    coeficientes existentes nesta base: Rendimento (CR), Progressão (CP) e Rendimento Padrão (CRP).
    
    A fórmula para cálculo do Score Final é:
    
        SC = (100 * (CR + CP)) / 2 + CRP
    
        Onde:
    
        SC  = Score Final
        CR  = Coeficiente de Rendimento. Consulte rendimento.py para mais detalhes
        CP  = Coeficiente de Progressão. Conslute progressao.py para mais detalhes
        CRP = Coeficiente de Rendimento Padrão. Conslute padrao.py para mais detalhes
    
        ** 1. CR e CP são multiplicados por 100 para que o Score seja de 0 a 100
        ** 2. CRP entra na conta como um bônus adicional na nota, uma vez que sua base é diferente das demais
        
    O Score final tem como objetivo ser uma forma de avaliar, ponderando diversos fatores, o nível e qualidade dos
    usuários da plataforma que fizeram provas. O score varia de 0 até 100+, pois o adicional dado pelo CRP pode colocar
    o índice um pouco acima do padrão.
    
    Como o cálculo é realizado a partir dos outros coeficientes, as funções abaixo dependem de rendimento.py,
    progressao.py e padrao.py para funcionarem. As variáveis globais abaixo são utilizadas para consolidar a tabela de
    scores. As funções permitem que se incluam filtros opcionais para se refinar os resultados e buscas.
    
"""

from coeficiente import rendimento, progressao, padrao

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

def por_funcional(funcional, categoria=None, base1=False):

    try:
        if categoria is not None:
            cr = rendimento.por_funcional(funcional, categoria=categoria)
            crp = padrao.por_funcional(funcional, categoria=categoria)
            cp = progressao.por_funcional(funcional, categoria=categoria)

            score_final = ((100 * cr) + (100 * cp)) / 2 + crp
        else:
            cr = rendimento.por_funcional(funcional)
            crp = padrao.por_funcional(funcional)
            cp = progressao.por_funcional(funcional)

            score_final = ((100 * cr) + (100 * cp)) / 2 + crp

    # Caso uma das buscas retorne em erro
    except NameError:
        return None

    if base1:
        return score_final / 100

    return score_final

def por_racf(racf, categoria=None, base1=False):

    try:
        if categoria is not None:
            cr = rendimento.por_racf(racf, categoria=categoria)
            crp = padrao.por_racf(racf, categoria=categoria)
            cp = progressao.por_racf(racf, categoria=categoria)

            score_final = ((100 * cr) + (100 * cp)) / 2 + crp
        else:
            cr = rendimento.por_racf(racf)
            crp = padrao.por_racf(racf)
            cp = progressao.por_racf(racf)

            score_final = ((100 * cr) + (100 * cp)) / 2 + crp

    # Caso uma das buscas retorne em erro
    except NameError:
        return None

    if base1:
        return score_final / 100

    return score_final

def por_nome(nome, categoria=None, base1=False):

    try:
        if categoria is not None:
            cr = rendimento.por_nome(nome, categoria=categoria)
            crp = padrao.por_nome(nome, categoria=categoria)
            cp = progressao.por_nome(nome, categoria=categoria)

            score_final = ((100 * cr) + (100 * cp)) / 2 + crp
        else:
            cr = rendimento.por_nome(nome)
            crp = padrao.por_nome(nome)
            cp = progressao.por_nome(nome)

            score_final = ((100 * cr) + (100 * cp)) / 2 + crp

    # Caso uma das buscas retorne em erro
    except NameError:
        return None

    if base1:
        return score_final / 100

    return score_final

'''
  Calcula todos os Scores de todos os usuários que realizaram ao menos uma prova e retorna uma lista ranqueada.
  Categoria é um filtro opcional e top20 retorna apenas os 20 primeiros colocados. Em caso de empate, o primeiro a fazer
as provas fica na frente.
  Por padrão, retorna nome e funcional. Mas a função também permite se escolher o que a tabela retornará. É possível,
ainda, retornar apenas o score, sem os outros valores.
'''
def gerar_tabela(categoria=None, nome=True, funcional=False, top20=False, apenas_final=False):

    try:

        temp = []
        tabela = pd.DataFrame()

        # Filtro por categoria
        if categoria is not None:

            lista_nomes = pd.read_sql_query(_query + "\nWHERE [categoria] = '" + categoria + "'", _conexao)
            lista_nomes = lista_nomes.drop_duplicates(subset=['nome', 'funcional'])

            for index, row in lista_nomes.iterrows():
                cr  = rendimento.por_nome(row['nome'], categoria=categoria)
                cp  = progressao.por_nome(row['nome'], categoria=categoria)
                crp = padrao.por_nome(row['nome'], categoria=categoria)

                if funcional and nome:
                    temp.append([row['nome'], row['funcional'], cr * 100, cp * 100, crp, ((100 * cr) + (100 * cp)) / 2 + crp])
                elif funcional and not nome:
                    temp.append([row['funcional'], cr * 100, cp * 100, crp, ((100 * cr) + (100 * cp)) / 2 + crp])
                elif not funcional and nome:
                    temp.append([row['nome'], cr * 100, cp * 100, crp, ((100 * cr) + (100 * cp)) / 2 + crp])
                else:
                    temp.append([cr * 100, cp * 100, crp, ((100 * cr) + (100 * cp)) / 2 + crp])

            if funcional and nome:
                tabela = tabela.append(pd.DataFrame(temp, columns=['Nome', 'Funcional', 'Rendimento', 'Progressão', 'Padrão de Rendimento', 'Score Final']))
            elif funcional and not nome:
                tabela = tabela.append(pd.DataFrame(temp, columns=['Funcional', 'Rendimento', 'Progressão', 'Padrão de Rendimento', 'Score Final']))
            elif not funcional and nome:
                tabela = tabela.append(pd.DataFrame(temp, columns=['Nome', 'Rendimento', 'Progressão', 'Padrão de Rendimento', 'Score Final']))
            else:
                tabela = tabela.append(pd.DataFrame(temp, columns=['Rendimento', 'Progressão', 'Padrão de Rendimento', 'Score Final']))

        # Sem filtro
        else:

            lista_nomes = pd.read_sql_query(_query, _conexao)
            lista_nomes = lista_nomes.drop_duplicates(subset=['nome', 'funcional'])

            for index, row in lista_nomes.iterrows():
                cr = rendimento.por_nome(row['nome'])
                cp = progressao.por_nome(row['nome'])
                crp = padrao.por_nome(row['nome'])

                if funcional and nome:
                    temp.append([row['nome'], row['funcional'], cr * 100, cp * 100, crp, ((100 * cr) + (100 * cp)) / 2 + crp])
                elif funcional and not nome:
                    temp.append([row['funcional'], cr * 100, cp * 100, crp, ((100 * cr) + (100 * cp)) / 2 + crp])
                elif not funcional and nome:
                    temp.append([row['nome'], cr * 100, cp * 100, crp, ((100 * cr) + (100 * cp)) / 2 + crp])
                else:
                    temp.append([cr * 100, cp * 100, crp, ((100 * cr) + (100 * cp)) / 2 + crp])

            if funcional and nome:
                tabela = tabela.append(pd.DataFrame(temp, columns=['Nome', 'Funcional', 'Rendimento', 'Progressão', 'Padrão de Rendimento', 'Score Final']))
            elif funcional and not nome:
                tabela = tabela.append(pd.DataFrame(temp, columns=['Funcional', 'Rendimento', 'Progressão', 'Padrão de Rendimento', 'Score Final']))
            elif not funcional and nome:
                tabela = tabela.append(pd.DataFrame(temp, columns=['Nome', 'Rendimento', 'Progressão', 'Padrão de Rendimento', 'Score Final']))
            else:
                tabela = tabela.append(pd.DataFrame(temp, columns=['Rendimento', 'Progressão', 'Padrão de Rendimento', 'Score Final']))

        tabela = tabela.rename_axis('ID').sort_values(by=['Score Final', 'ID'], ascending=[False, True])

        # Apenas os 20 primeiros
        if top20:
            tabela = tabela.head(20)

        if apenas_final:
            if funcional and nome:
                tabela = tabela['ID', 'Nome', 'Funcional', 'Final']
            elif funcional and not nome:
                tabela = tabela['ID', 'Funcional', 'Final']
            elif not funcional and nome:
                tabela = tabela['ID', 'Nome', 'Final']
            else:
                tabela = tabela['ID', 'Final']

    # Erro nas queries de seleção
    except pd.io.sql.DatabaseError:
        return None

    # Ranking vazio
    if tabela.empty:
        return None

    return tabela
