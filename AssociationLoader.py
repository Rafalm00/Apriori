import os
import pickle
import FileScanner as fs
import pandas as pd
import numpy as np
import AprioriPreLoader as APL
from mlxtend.frequent_patterns import association_rules


def non_symmetric_associations(store_number, confidence_threshold=0.3, apriori_threshold=0, concat_items=False):
    cwd = os.getcwd()
    if concat_items:
        itemconcat = "_T"
    else:
        itemconcat = "_F"
    work_path = cwd + "/apriori/"
    output_path = cwd + "/output/"
    store = "vendas_" + str(store_number)

    available_folders = []
    for file_name in os.listdir(cwd + "/apriori"):
        if ("vendas_" + str(store_number) in file_name) and (itemconcat in file_name)and ("_ok" in file_name):
            available_folders.append(float(file_name.split("_")[2]))
    if len(available_folders) == 0:
        print("Nenhum arquivo pronto para ser processado")
        return
    else:
        if apriori_threshold == 0:
            available_folders = sorted(available_folders)
            folder_name = store + "_" + str(available_folders[0]) + itemconcat + "_ok"
        else:
            if apriori_threshold in available_folders:
                folder_name = store + "_" + str(apriori_threshold) + itemconcat + "_ok"
            else:
                print("Nenhum arquivo pronto para ser processado com o valor desejado de threshold")
                return
    del available_folders
    with open(work_path + folder_name + "/globaldict.pickle", 'rb') as handle:
        global_dict = {}
        global_dict = pickle.load(handle)

    association_list = pd.DataFrame()
    for file_name in os.listdir(work_path + folder_name):
        week_now = file_name.split(".")[0]
        if not file_name.endswith(".pickle"):
            df = pd.read_pickle(work_path + folder_name + "/" + file_name)
            df = association_rules(df, metric="confidence", min_threshold=confidence_threshold)
            df["antecedent_len"] = df["antecedents"].apply(lambda a: len(a))
            df["consequent_len"] = df["consequents"].apply(lambda a: len(a))
            df = df[(df["antecedent_len"] == 1) & (df["consequent_len"] == 1)]
            df["antecedent_lucro"] = df["antecedents"].apply(lambda a: (global_dict[week_now][str(a).split("'")[1]]["lucro"] /global_dict[week_now][str(a).split("'")[1]]["quant"]) * global_dict[week_now]["quant_of_transactions"])
            df["antecedent_fat"] = df["antecedents"].apply(lambda a: (global_dict[week_now][str(a).split("'")[1]]["faturamento"] /global_dict[week_now][str(a).split("'")[1]]["quant"]) * global_dict[week_now]["quant_of_transactions"])
            df["consequent_lucro"] = df["consequents"].apply(lambda a: (global_dict[week_now][str(a).split("'")[1]]["lucro"] /global_dict[week_now][str(a).split("'")[1]]["quant"]) * global_dict[week_now]["quant_of_transactions"])
            df["consequent_fat"] = df["consequents"].apply(lambda a: (global_dict[week_now][str(a).split("'")[1]]["faturamento"] /global_dict[week_now][str(a).split("'")[1]]["quant"]) * global_dict[week_now]["quant_of_transactions"])
            df["lucro"] = (df["antecedent_lucro"] + df["consequent_lucro"]) * df["support"]
            df["faturamento"] = (df["antecedent_fat"] + df["consequent_fat"]) * df["support"]
            df["data"] = week_now
            df["quant"] = df["support"].apply(lambda x: int(x * global_dict[week_now]["quant_of_transactions"]))
            if len(association_list) == 0:
                association_list = df
            else:
                print(week_now + " foi processada e adicionada a lista")
                association_list = pd.concat([association_list, df])

    association_list["Nconfidence"] = association_list["support"] / association_list["consequent support"]
    association_list["confidenceDif"] = abs(association_list["confidence"] - association_list["Nconfidence"])
    association_list = association_list.drop(columns=["antecedent_len", "consequent_len", "antecedent_lucro", "antecedent_fat", "consequent_lucro", "consequent_fat"])
    association_list = association_list.reindex(["antecedents", "consequents", "antecedent support", "consequent support", "support", "confidence", "Nconfidence", "confidenceDif", "lucro", "faturamento", "lift", "leverage", "conviction", "data"], axis=1)
    association_list = association_list.reset_index(drop=True)
    association_list.to_csv(output_path + "associationList_" + store + "_" + str(confidence_threshold).replace(".", ",") + itemconcat + "_Nconf" + ".csv", sep=';', encoding='utf-8', index=False)
    return


def generate_avg_cupom_size(store_number, hour=True, freq='W'):
    file_path = "vendas_" + str(store_number) + ".txt"
    input_path = os.getcwd() + "/input/"
    output_path = os.getcwd() + "/output/"
    vendas = fs.sort_file(input_path + file_path, hour)
    index = fs.get_daterange(input_path + file_path, hour, freq=freq)
    pd.set_option('display.width', 320)
    pd.set_option("display.max_columns", 15)
    x = []
    sigma = []
    cupomsizeavg = []
    aux_list = []
    less_then_ten = []
    cont = 0
    week_now = ''
    faturamento_list = []
    aux_faturamento = 0.0

    for venda in vendas:
        if hour:
            dia, mes, ano = venda.split(";")[1].split(" ")[0].split("/")
        else:
            dia, mes, ano = venda.split(";")[1].split("/")
        date_now = ano + "-" + mes + "-" + dia

        if date_now in index:
            if date_now != week_now and week_now != '':
                faturamento_list.append(aux_faturamento)
                aux_faturamento = 0.0
                sigma.append(np.std(aux_list))
                cupomsizeavg.append(np.mean(aux_list))
                x.append(week_now)
                for number in aux_list:
                    if number <= 10:
                        cont = cont + 1
                less_then_ten.append(cont/len(aux_list))
                cont = 0
                print("semana " + week_now + " concluida")
                aux_list.clear()
            week_now = date_now
        if week_now in index:
            aux_list.append(len(venda.split(";")[0].split("}")))
            for splitedString in venda.split(";")[0].split("}"):
                if splitedString.split(",")[3] == "nan":
                    break
                aux_faturamento = aux_faturamento + float(splitedString.split(",")[3])

    df = pd.DataFrame()
    df["data"] = x
    df["sigma"] = sigma
    df["avg cupom"] = cupomsizeavg
    df["faturamento"] = faturamento_list
    df["less than ten"] = less_then_ten
    df.to_csv(output_path + "avgSigma_vendas_" + str(store_number) + ".csv", sep=';', encoding='utf-8', index=False)
    return


def generate_component_list(store_number=0, apriori_threshold=0, concat_items=False, file_path="none"):
    work_path = os.getcwd() + "/apriori/"
    output_path = os.getcwd() + "/output/"
    if concat_items:
        itemconcat = "_T"
    else:
        itemconcat = "_F"
    if file_path == "none":
        if not os.path.exists(work_path + "vendas_" + str(store_number) + "_" + str(apriori_threshold) + itemconcat + "_ok"):
            print("path especificado nao explicado")
            return
        else:
            file_path = work_path + "vendas_" + str(store_number) + "_" + str(apriori_threshold) + itemconcat + "_ok"
    store_number = file_path.split("vendas_")[1].split("_")[0]
    apriori_threshold = file_path.split("vendas_")[1].split("_")[1]
    itemconcat = file_path.split("vendas_")[1].split("_")[2]
    if not os.path.exists(file_path):
        print("path especificado nao explicado")
        return
    else:
        global_dict = pd.read_pickle(file_path + "/globaldict.pickle")
        week_list, itens_list, componente_list, lucro_list, faturamento_list, quantidade_list, store_number_list = [], [], [], [], [], [], []
        for week in global_dict.keys():
            for itens in global_dict[week].keys():
                if itens == "quant_of_transactions":
                    continue
                for componente in global_dict[week][itens]["componentes"].keys():
                    week_list.append(week)
                    itens_list.append(itens)
                    store_number_list.append(store_number)
                    componente_list.append(componente)
                    lucro_list.append(global_dict[week][itens]["componentes"][componente]["lucro"])
                    faturamento_list.append(global_dict[week][itens]["componentes"][componente]["faturamento"])
                    quantidade_list.append(global_dict[week][itens]["componentes"][componente]["quant"])
    df = pd.DataFrame()
    df["loja"] = store_number_list
    df["week"] = week_list
    df["item"] = itens_list
    df["componente"] = componente_list
    df["lucro"] = lucro_list
    df["faturamento"] = faturamento_list
    df["quant"] = quantidade_list
    df.to_csv(output_path + "componentList_vendas_" + store_number + "_" + apriori_threshold + "_" + itemconcat + ".csv", sep=';', encoding='utf-8', index=False)

    return


def compare_real_rand(store_number, confidence_threshold=0.3, concat_items=False):
    if concat_items:
        itemconcat = "_T"
    else:
        itemconcat = "_F"
    cwd = os.getcwd()
    output_path = cwd + "/input/"
    pd.set_option('display.width', 320)
    pd.set_option("display.max_columns", 15)

    dfreal = pd.read_csv(output_path + "associationList_vendas_" + str(store_number) + "_" + str(confidence_threshold).replace(".", ",") + itemconcat + "_Nconf.csv", sep=";", encoding="utf-8")
    dfrandom = pd.read_csv(output_path + "associationList_vendas_" + "rand" + str(store_number) + "_" + str(confidence_threshold).replace(".", ",") + itemconcat + "_Nconf.csv", sep=";", encoding="utf-8")
    print(dfreal)
    print(dfrandom)
    confidence_list = []
    nconfidence_list = []
    dicti = {}

    for i, association in dfrandom.iterrows():
        if not association["data"] in dicti.keys():
            dicti[association["data"]] = {}
        dicti[association["data"]][association["antecedents"] + " -> " + association["consequents"]] = {"confidence": association["confidence"], "Nconfidence": association["Nconfidence"]}

    for i, association in dfreal.iterrows():
        if not association["data"] in dicti.keys():
            confidence_list.append(0)
            nconfidence_list.append(0)
        else:
            if not (association["antecedents"] + " -> " + association["consequents"]) in dicti[association["data"]].keys():
                confidence_list.append(0)
                nconfidence_list.append(0)
            else:
                confidence_list.append(dicti[association["data"]][association["antecedents"] + " -> " + association["consequents"]]["confidence"])
                nconfidence_list.append(dicti[association["data"]][association["antecedents"] + " -> " + association["consequents"]]["Nconfidence"])

    dfreal["confidenceRand"] = confidence_list
    dfreal["NconfidenceRand"] = nconfidence_list
    dfreal["conf{real-random}"] = dfreal["confidence"] - dfreal["confidenceRand"]
    dfreal["Nconf{real-random}"] = dfreal["Nconfidence"] - dfreal["NconfidenceRand"]

    dfreal.to_csv(output_path + "associationList_vendas_" + str(store_number) + "_" + str(confidence_threshold).replace(".", ",") + itemconcat + "_Nconf_RandConf" + ".csv", sep=';', encoding='utf-8', index=False)
    return


def load_multiple_associations(store_numbers, confidence_threshold=0.3, apriori_threshold=0, concat_items=False):
    for store in store_numbers:
        non_symmetric_associations(store, confidence_threshold=confidence_threshold, apriori_threshold=apriori_threshold, concat_items=concat_items)
    return


def load_multiple_files(store_number=[], list_of_selections=[], hour=True, apriori_threshold=0.01, confidence_threshold=0.3, itemconcat='_F', freq='W'):
    # if len(list_of_selections) != 0:
    #     for inner_list in list_of_selections:
    #         if len(inner_list) != 3:
    #             print("porfavor aplicar uma lista de seleção com formatação correta, a formatação deve ser feita da seguinte maneira: "
    #                   "cada componente da lista deve ser feita por 3 lista que irá definir seções separadas por grupo, subseção e item respectivamente")
    #             return
    #     for i, store in enumerate(store_number):
    #         APL.loadfile(store, by_group=list_of_selections[i][0], by_subsections=list_of_selections[i][1], by_itens=list_of_selections[i][2], apriori_threshold=apriori_threshold, itemconcat=itemconcat, hour=hour, freq=freq)
    # else:
    #     for store in store_number:
    #         APL.loadfile(store, apriori_threshold=apriori_threshold, itemconcat=itemconcat, hour=hour, freq=freq)
    for store in store_number:
        while True:
            try:

                break
            except MemoryError:
                if apriori_threshold == 0.10:
                    print() #deal with it
                apriori_threshold = apriori_threshold + 0.01
                continue
        apriori_threshold = 0.01

    return


def concat_csv_real_rand(store_numbers, confidence_threshold=0.3, concat_items=False):
    if concat_items:
        itemconcat = "_T"
    else:
        itemconcat = "_F"
    output_path = os.getcwd() + "/output/"
    list_of_csv = []
    for number in store_numbers:
        for file_name in os.listdir(os.getcwd() + "/output"):
            if ("associationList_vendas_" + str(number) + "_" + str(confidence_threshold).replace(".", ",") + itemconcat + "_Nconf" + ".csv") == file_name:
                list_of_csv.append(file_name)
                break
        else:
            print("arquivo loja " + number + " nao encontrado, continuando o processo sem incluir a loja")
    joint_path = output_path + "/ListOfAssociations_vendas_" + ",".join(str(x) for x in store_numbers) + "_" + str(confidence_threshold).replace(".", ",") + itemconcat + "_Nconf" + ".csv"
    if os.path.exists(joint_path):
        os.remove(joint_path)
    for path in list_of_csv:
        df = pd.read_csv(output_path + path)
        df["loja"] = str(path).split("_")[2]
        df.to_csv(joint_path, mode='a', index=False)
    return
