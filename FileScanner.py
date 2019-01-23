import pandas as pd
import os
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import apriori


def get_daterange(filepath, hour, freq='W'):
    ano = 9999
    file = open(filepath, 'r')
    for venda in file.readlines()[1:]:
        if venda.split(";")[1] != "nan\n":
            if hour:
                if int(venda.split(";")[1].split(" ")[0].split("/")[2]) < int(ano):
                    dia, mes, ano = venda.split(";")[1].split(" ")[0].split("/")
                elif int(venda.split(";")[1].split(" ")[0].split("/")[2]) == int(ano):
                    if int(venda.split(";")[1].split(" ")[0].split("/")[1]) < int(mes):
                        dia, mes, ano = venda.split(";")[1].split(" ")[0].split("/")
                    elif int(venda.split(";")[1].split(" ")[0].split("/")[1]) == int(mes):
                        if int(venda.split(";")[1].split(" ")[0].split("/")[0]) < int(dia):
                            dia, mes, ano = venda.split(";")[1].split(" ")[0].split("/")
            else:
                if int(venda.split(";")[1].split("/")[2]) < int(ano):
                    dia, mes, ano = venda.split(";")[1].split("/")
                elif int(venda.split(";")[1].split("/")[2]) == int(ano):
                    if int(venda.split(";")[1].split("/")[1]) < int(mes):
                        dia, mes, ano = venda.split(";")[1].split("/")
                    elif int(venda.split(";")[1].split("/")[1]) == int(mes):
                        if int(venda.split(";")[1].split("/")[0]) < int(dia):
                            dia, mes, ano = venda.split(";")[1].split("/")
    startDate = pd.datetime(int(ano), int(mes), int(dia))
    file.seek(0)
    for venda in file.readlines()[1:]:
        if venda.split(";")[1] != "nan\n":
            if hour:
                if int(venda.split(";")[1].split(" ")[0].split("/")[2]) > int(ano):
                    dia, mes, ano = venda.split(";")[1].split(" ")[0].split("/")
                elif int(venda.split(";")[1].split(" ")[0].split("/")[2]) == int(ano):
                    if int(venda.split(";")[1].split(" ")[0].split("/")[1]) > int(mes):
                        dia, mes, ano = venda.split(";")[1].split(" ")[0].split("/")
                    elif int(venda.split(";")[1].split(" ")[0].split("/")[1]) == int(mes):
                        if int(venda.split(";")[1].split(" ")[0].split("/")[0]) > int(dia):
                            dia, mes, ano = venda.split(";")[1].split(" ")[0].split("/")
            else:
                if int(venda.split(";")[1].split("/")[2]) > int(ano):
                    dia, mes, ano = venda.split(";")[1].split("/")
                elif int(venda.split(";")[1].split("/")[2]) == int(ano):
                    if int(venda.split(";")[1].split("/")[1]) > int(mes):
                        dia, mes, ano = venda.split(";")[1].split("/")
                    elif int(venda.split(";")[1].split("/")[1]) == int(mes):
                        if int(venda.split(";")[1].split("/")[0]) > int(dia):
                            dia, mes, ano = venda.split(";")[1].split("/")
    finalDate = pd.datetime(int(ano), int(mes), int(dia))
    index = pd.date_range(startDate, finalDate, freq=freq)
    file.close()
    return index


def sort_file(filepath, hour):
    vendas = []
    aux_vendas = []
    file = open(filepath, 'r')
    for venda in file.readlines()[1:]:
        if venda.split(";")[1] != "nan\n":
            if hour:
                dia, mes, ano = venda.split(";")[1].split(" ")[0].split("/")
            else:
                dia, mes, ano = venda.split(";")[1].split("/")
            timestamp = ano.rstrip() + mes + dia
            aux_vendas.append([venda.rstrip(), int(timestamp)])
    file.close()

    aux_vendas = sorted(aux_vendas, key=lambda r: r[1])
    for row in aux_vendas:
        del row[1]
    for line in aux_vendas:
        vendas.append(line[0])
    del aux_vendas
    return vendas


def file_constructor(vendas, week, folder_path, apriori_threshold=0.01):
    te = TransactionEncoder()
    if not os.path.exists(folder_path + "/" + week + "_ok.pkl"):
        te_ary = te.fit(vendas).transform(vendas, sparse=True)
        df = pd.SparseDataFrame(te_ary, columns=te.columns_, default_fill_value=False)
        del te_ary
        frequent_item_set = apriori(df, min_support=apriori_threshold, use_colnames=True)
        print("frequent itemSets semana " + week + ": ")
        print(frequent_item_set)
        df = pd.DataFrame(frequent_item_set)
        df.to_pickle(folder_path + "/" + week + "_ok.pkl")
        return df
    else:
        print(week + " já estava previamente processado")
    return


def transaction_handler(venda, week, globaldict={}, byitens=[], bygroup=[], bysubsection=[], itens_to_concat=[], concatitens=False):
    if "quant_of_transactions" not in globaldict[week].keys():
        globaldict[week]["quant_of_transactions"] = 0
    globaldict[week]["quant_of_transactions"] = globaldict[week]["quant_of_transactions"] + 1
    itens_to_append = []
    if len(itens_to_concat) != 0:
        concatitens = True
    for item in venda.split(";")[0].split("}"):
        lucro, faturamento = str(item).split("{")[1].split(",")
        item_id = str(item).split("(")[0]
        section_id, group_id, subsection_id = str(item).split("(")[1].split(")")[0].split(",")

        if int(section_id) in byitens:
            if concatitens:
                for conjunto in itens_to_concat:
                    for aux_id in conjunto:
                        if int(item_id) == aux_id:
                            new_item = conjunto[0]
            else:
                new_item = item_id
        elif int(section_id) in bygroup:
            # if concatitens:
            #     for conjunto in itens_to_concat:
            #         for aux_id in conjunto:
            #             if group_id == aux_id:
            #                 new_item = conjunto[0]
            # else:
            #     new_item = group_id
            new_item = section_id + "-" + group_id + "-0"
        elif int(section_id) in bysubsection:
            # if concatitens:
            #     for conjunto in itens_to_concat:
            #         for aux_id in conjunto:
            #             if subsection_id == aux_id:
            #                 new_item = conjunto[0]
            # else:
            #     new_item = subsection_id
            new_item = section_id + "-" + group_id + "-" + subsection_id
        else:
            # if concatitens:
            #     for conjunto in itens_to_concat:
            #         for aux_id in conjunto:
            #             if section_id == aux_id:
            #                 new_item = conjunto[0]
            # else:
            #     new_item = section_id
            new_item = section_id + "-0-0"

        if new_item not in globaldict[week].keys():
            globaldict[week][new_item] = {"lucro": 0, "faturamento": 0, "quant": 0, "componentes": {}}
        globaldict[week][new_item]["lucro"] = globaldict[week][new_item]["lucro"] + float(lucro)
        globaldict[week][new_item]["faturamento"] = globaldict[week][new_item]["faturamento"] + float(faturamento)
        globaldict[week][new_item]["quant"] = globaldict[week][new_item]["quant"] + 1
        if item_id != new_item:
            if item_id not in globaldict[week][new_item]["componentes"].keys():
                globaldict[week][new_item]["componentes"][item_id] = {"lucro": 0, "faturamento": 0, "quant": 0}
            globaldict[week][new_item]["componentes"][item_id]["lucro"] = globaldict[week][new_item]["componentes"][item_id]["lucro"] + float(lucro)
            globaldict[week][new_item]["componentes"][item_id]["faturamento"] = globaldict[week][new_item]["componentes"][item_id]["faturamento"] + float(faturamento)
            globaldict[week][new_item]["componentes"][item_id]["quant"] = globaldict[week][new_item]["componentes"][item_id]["quant"] + 1
        if new_item not in itens_to_append:
            itens_to_append.append(new_item)

    return itens_to_append.copy()
