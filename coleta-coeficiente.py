from coeficiente import final
import pandas as pd
'''
ranking_vba = final.gerar_tabela(categoria='03 - VBA - Visual Basic', funcional=True)
ranking_vba.to_excel('ranking_vba.xlsx', sheet_name='Ranking VBA')

ranking_java = final.gerar_tabela(categoria='10 - Java', funcional=True)
ranking_java.to_excel('ranking_java.xlsx', sheet_name='Ranking Java')
'''
ranking_python = final.gerar_tabela(categoria='04 - Python', funcional=True)
ranking_python.to_excel('ranking_python.xlsx', sheet_name='Ranking Python')
'''
ranking_js = final.gerar_tabela(categoria='13 - JavaScript', funcional=True)
ranking_js.to_excel('ranking_js.xlsx', sheet_name='Ranking JavaScript')

rankig_geral = final.gerar_tabela(funcional=True)
rankig_geral.to_excel('ranking_geral.xlsx', sheet_name="Ranking Geral")
'''

