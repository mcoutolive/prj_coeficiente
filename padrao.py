import numpy as np
import pandas as pd
import pandas.io.sql
from coeficiente import rendimento

"""
    Neste arquivo, estão as funções para cálculo do Coeficiente de Rendimento Padrão (CRP)

    A fórmula para cálculo do CRP é:

        CRP = (CR + MCR) / DpCRT

        Onde:

        CRP   = Coeficiente de Rendimento Padrão
        CR    = Coeficiente de Rendimento (veja o import da linha 2 patra mais detalhes)
        MCR   = Média de CRs de todos os usuários com prova
        DpCRT = Desvio padrão de todos os CRs de usuários com prova
        
        ******* Esta conta é feita desta forma pois assim permite padronizar os dados de acordo com a média, que, neste
                caso, é zero

    O CRP permite ver, utilizando a base de CRs, o quaão destacado o usuário está dos demais. Este índice pode variar
    entre valores positivos e negativos. Quanto mais próximo de 0, mais próximo o usuário está da média de CRs. Quanto
    mais distante, maior é o destaque dele aos demais, seja positiva ou negativamente.

    As funções permitem que se utilize filtros opcionais para se refinar a busca. Este arquivo também se utiliza do
    arquivo de coeficientes para realizar seus cálculos

"""

"""FUNÇÕES"""

# Calcula o CRP de um usuário fazendo a busca via FUNCIONAL. Categoria é um filtro opcional
def por_funcional(funcional, categoria=None):

    try:

        # Filtro por categoria
        if categoria is not None:
            cr = rendimento.por_funcional(funcional, categoria=categoria)
            media_total = rendimento.rendimento_medio(categoria=categoria)

            media_turma = rendimento.ranking_coeficentes(categoria=categoria)
            media_turma = media_turma['Coeficiente']

            coeficiente = (cr - media_total) / np.std(media_turma)

        # Sem filtro
        else:
            cr = rendimento.por_funcional(funcional=funcional)
            media_total = rendimento.rendimento_medio()

            media_turma = rendimento.ranking_coeficentes()
            media_turma = media_turma['Coeficiente']

            coeficiente = (cr - media_total) / np.std(media_turma)

    # Caso uma das buscas retorne em erro
    except NameError:
        return None

    return coeficiente

# Calcula o CRP de um usuário fazendo a busca via RACF. Categoria é um filtro opcional
def por_racf(racf, categoria=None):

    try:

        # Filtro por categoria
        if categoria is not None:
            cr = rendimento.por_racf(racf, categoria=categoria)
            media_total = rendimento.rendimento_medio(categoria=categoria)

            media_turma = rendimento.ranking_coeficentes(categoria=categoria)
            media_turma = media_turma['Coeficiente']

            coeficiente = (cr - media_total) / np.std(media_turma)

        # Sme filtro
        else:
            cr = rendimento.por_racf(racf=racf)
            media_total = rendimento.rendimento_medio()

            media_turma = rendimento.ranking_coeficentes()
            media_turma = media_turma['Coeficiente']

            coeficiente = (cr - media_total) / np.std(media_turma)

    # Caso uma das buscas retorne em erro
    except NameError:
        return None

    return coeficiente

# Calcula o CRP de um usuário fazendo a busca via NOME. Categoria é um filtro opcional
def por_nome(nome, categoria=None):

    try:

        # Filtro por categoria
        if categoria is not None:
            cr = rendimento.por_nome(nome, categoria=categoria)
            media_total = rendimento.rendimento_medio(categoria=categoria)

            media_turma = rendimento.ranking_coeficentes(categoria=categoria)
            media_turma = media_turma['Coeficiente']

            coeficiente = (cr - media_total) / np.std(media_turma)

        # Sem filtro
        else:
            cr = rendimento.por_nome(nome=nome)
            media_total = rendimento.rendimento_medio()

            media_turma = rendimento.ranking_coeficentes()
            media_turma = media_turma['Coeficiente']

            coeficiente = (cr - media_total) / np.std(media_turma)

    # Caso uma das buscas retorne em erro
    except NameError:
        return None

    return coeficiente

'''
  Calcula todos os CRs de todos os usuários que realizaram ao menos uma prova e retorna uma lista ranqueada.
  Categoria é um filtro opcional e top20 retorna apenas os 20 primeiros colocados. Em caso de empate, o primeiro a fazer
as provas fica na frente.
  Por padrão, retorna nome e funcional. Mas a função também permite se escolher o que a tabela retornará. É possível,
ainda, retornar apenas o score, sem os outros valores.
'''
def ranking_padrao(categoria=None, nome=True, funcional=False, top20=False):

    try:

        temp = []
        ranking = pd.DataFrame()

        # Filtro categoria
        if categoria is not None:

            lista_cr = rendimento.ranking_coeficentes(categoria=categoria, nome=True, funcional=True)
            desvio = np.std(lista_cr['Coeficiente'])
            media_total = rendimento.rendimento_medio(categoria=categoria)

            for index, row in lista_cr.iterrows():

                coeficiente = (row['Coeficiente'] - media_total) / desvio

                if funcional and nome:
                    temp.append([row['Nome'], row['Funcional'], coeficiente])
                elif funcional and not nome:
                    temp.append([row['Funcional'], coeficiente])
                elif not funcional and nome:
                    temp.append([row['Nome'], coeficiente])
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

            lista_cr = rendimento.ranking_coeficentes(nome=True, funcional=True)
            desvio = np.std(lista_cr['Coeficiente'])
            media_total = rendimento.rendimento_medio()

            for index, row in lista_cr.iterrows():

                coeficiente = (row['Coeficiente'] - media_total) / desvio

                if funcional and nome:
                    temp.append([row['Nome'], row['Funcional'], coeficiente])
                elif funcional and not nome:
                    temp.append([row['Funcional'], coeficiente])
                elif not funcional and nome:
                    temp.append([row['Nome'], coeficiente])
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
