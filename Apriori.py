import pandas as pd
from mlxtend.preprocessing import TransactionEncoder
import matplotlib.pyplot as plt
from mlxtend.frequent_patterns import apriori
from mlxtend.frequent_patterns import association_rules

desired_width = 320
pd.set_option('display.width', desired_width)
pd.set_option("display.max_columns", 10)

file = open("vendas_25(section,group,subsection).txt", "r")
auxVendas = []
vendas = []
ano = 9999

for venda in file.readlines()[1:]:
    if venda.split(";")[1] != "nan\n":
        if int(venda.split(";")[1].split(" ")[0].split("/")[2]) < int(ano):
            dia, mes, ano = venda.split(";")[1].split(" ")[0].split("/")
        elif int(venda.split(";")[1].split(" ")[0].split("/")[2]) == int(ano):
            if int(venda.split(";")[1].split(" ")[0].split("/")[1]) < int(mes):
                dia, mes, ano = venda.split(";")[1].split(" ")[0].split("/")
            elif int(venda.split(";")[1].split(" ")[0].split("/")[1]) == int(mes):
                if int(venda.split(";")[1].split(" ")[0].split("/")[0]) < int(dia):
                    dia, mes, ano = venda.split(";")[1].split(" ")[0].split("/")
startDate = pd.datetime(int(ano), int(mes), int(dia))
file.seek(0)
weekDict = {}

# procurando data final
for venda in file.readlines()[1:]:
    if venda.split(";")[1] != "nan\n":
        if int(venda.split(";")[1].split(" ")[0].split("/")[2]) > int(ano):
            dia, mes, ano = venda.split(";")[1].split(" ")[0].split("/")
        elif int(venda.split(";")[1].split(" ")[0].split("/")[2]) == int(ano):
            if int(venda.split(";")[1].split(" ")[0].split("/")[1]) > int(mes):
                dia, mes, ano = venda.split(";")[1].split(" ")[0].split("/")
            elif int(venda.split(";")[1].split(" ")[0].split("/")[1]) == int(mes):
                if int(venda.split(";")[1].split(" ")[0].split("/")[0]) > int(dia):
                    dia, mes, ano = venda.split(";")[1].split(" ")[0].split("/")
finalDate = pd.datetime(int(ano), int(mes), int(dia))
file.seek(0)

for venda in file.readlines()[1:]:
    if venda.split(";")[1] != "nan\n":
        dia, mes, ano = venda.split(";")[1].split(" ")[0].split("/")
        timestamp = ano.rstrip() + mes + dia
        auxVendas.append([venda.rstrip(), int(timestamp)])
file.close()


auxVendas = sorted(auxVendas, key=lambda r: r[1])
for row in auxVendas:
    del row[1]
for line in auxVendas:
    vendas.append(line[0])
del auxVendas


# gerando dia inicial de cada semana do periodo dado
index = pd.date_range(startDate, finalDate, freq='W')

print(index)

getterFactor = [10]

byGroup = [1, 15, 12, 10, 17]
bySubSection = [5, 18]
byItens = [13, 14]
# qualquer um encontrado que nao esteja nessas listas é tratado para adicionar apenas o número de sua sessão

count = 0
Xaxis = []  # x do grafico a ser analisado ( quantidade de itens pegues como comuns de cada semana)
Yaxis = []  # y do grafico a ser analisado ( quantidade de itens pegues como comuns do final)
te = TransactionEncoder()
mostCommonItemsets = []
dfHigherConfidencePerWeek = []

auxVendas = []
itensToAppend = []
itemToAppend = []
for actQuant in getterFactor:
    firstDay = 0
    auxVendas.clear()
    for venda in vendas:
        dia, mes, ano = venda.split(";")[1].split(" ")[0].split("/")
        if (ano + "-" + mes + "-" + dia in index) and (firstDay != int(dia)):                   # checagem se o proximo dia a ser analisado chegou
            if firstDay != 0:                                                                   # nao e realizado a analise da primeira semana, pois ela esta incompleta
                te_ary = te.fit(auxVendas).transform(auxVendas)
                df = pd.DataFrame(te_ary, columns=te.columns_)
                frequentItemSet = apriori(df, min_support=0.01, use_colnames=True)              # aplicando apriori assim que o inicio da proxima semana chegar
                associationsList = association_rules(frequentItemSet, metric="confidence", min_threshold=0.5)
                df = pd.DataFrame(associationsList)
                df = df.sort_values(by="confidence", ascending=False)
                df["week"] = lastPrintedDay
                print(" Lista de itemsets comuns da semana " + lastPrintedDay + ":")
                if len(dfHigherConfidencePerWeek) == 0:
                    dfHigherConfidencePerWeek = pd.DataFrame(df.head(actQuant))
                else:
                    df2 = pd.DataFrame(df.head(actQuant))
                    dfHigherConfidencePerWeek = pd.concat([dfHigherConfidencePerWeek, df2])
                dfHigherConfidencePerWeek = dfHigherConfidencePerWeek.reset_index(drop=True)
                print(dfHigherConfidencePerWeek)
                ndf = df[["antecedents", "consequents"]]
                # print(assosiationsList)
                #frequentItemSet['length'] = frequentItemSet['itemsets'].apply(lambda x: len(x)) # definindo tamanho de cada itemset
                #df = pd.DataFrame(frequentItemSet)                                              # transformando a saida do apriori de frozenset para dataframe
                #df = df[df["length"] >= 2]                                                      # filtrando os valores para apenas itemse de tamanho 2 ou mais
                # df["nSupport"] = df["support"] * df["length"]                                 # gerando um novo suporte ponderado baseado no tamanho e suporte de cada itemset
                #df = df.sort_values(by="support", ascending=False)                              # sorteando a lista baseada no novo supore

                #ndf = df["itemsets"].tolist()
                for i, (_, itemset) in enumerate(ndf.iterrows()):
                    if i >= actQuant:
                        break
                    strItemset = str(itemset["antecedents"]).split("(")[1].split(")")[0] + " --> " + str(itemset["consequents"]).split("(")[1].split(")")[0]
                    if strItemset not in mostCommonItemsets:
                        mostCommonItemsets.append(strItemset)   # adiquirindo os itemsets mais comuns de todas semanas e adicionando eles em uma lista
                print(mostCommonItemsets)
                print(len(mostCommonItemsets))
                Yaxis.append(len(mostCommonItemsets))
                if count not in Xaxis:
                    Xaxis.append(count)
                count = count + 1
            auxVendas.clear()
            firstDay = int(dia)
            lastPrintedDay = dia + "/" + mes + "/" + ano
        for splitedString in venda.split(");")[0].split("),"):                                  # separacao de cada linha em itens, grupo, etc... baseado na escolha feita anteriormente
            itemToAppend = splitedString.split("(")[1].split(",")
            if int(itemToAppend[0]) in byGroup:
                # itensToAppend.append(itemToAppend[0] + "-0-0")
                itensToAppend.append(itemToAppend[0] + "-" + itemToAppend[1] + "-0")
            elif int(itemToAppend[0]) in bySubSection:
                # itensToAppend.append(itemToAppend[0] + "-0-0")
                # itensToAppend.append(itemToAppend[0] + "-" + itemToAppend[1] + "-0")
                itensToAppend.append(itemToAppend[0] + "-" + itemToAppend[1] + "-" + itemToAppend[2])
            elif int(itemToAppend[0]) in byItens:
                # itensToAppend.append(itemToAppend[0] + "-0-0")
                # itensToAppend.append(itemToAppend[0] + "-" + itemToAppend[1] + "-0")
                # itensToAppend.append(itemToAppend[0] + "-" + itemToAppend[1] + "-" + itemToAppend[2])
                itensToAppend.append(splitedString.split("(")[0])
            else:
                itensToAppend.append(itemToAppend[0] + "-0-0")
        auxVendas.append(itensToAppend.copy())
        itensToAppend.clear()
    print("")
    print("Lista de itens mais comuns: ")  # lista dos itemsets mais comuns obtidos apartir de todas as analises feitas
    plt.plot(Xaxis, Yaxis)
    Xaxis.clear()
    Yaxis.clear()
    mostCommonItemsets.clear()
    cont = 0
    dfHigherConfidencePerWeek.reset_index(drop=True)
dfHigherConfidencePerWeek.to_csv("HigherConfidencePerWeek", sep=';', encoding='utf-8', index=False)
plt.show()
