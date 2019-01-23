import os
import pickle
import FileScanner as fs


def prepare_workspace():
    cwd = os.getcwd()
    if not os.path.exists(cwd + "/input"):
        os.mkdir(cwd + "/input")
    if not os.path.exists(cwd + "/output"):
        os.mkdir(cwd + "/output")
    if not os.path.exists(cwd + "/apriori"):
        os.mkdir(cwd + "/apriori")
    return


def loadfile(store_number, by_group=[], by_subsections=[], by_itens=[], itens_to_concat=[], hour=True, apriori_threshold=0.01, itemconcat='_F', freq='W'):
    prepare_workspace()
    file_path = "vendas_" + str(store_number) + ".txt"
    cwd = os.getcwd()
    imput_path = cwd + "/input/"
    work_path = cwd + "/apriori/"
    print(cwd)
    global_dict = {}
    if len(itens_to_concat) != 0:
        itemconcat = "_T"
    folder_name = file_path.split(".")[0] + "_" + str(apriori_threshold) + itemconcat

    if os.path.exists(work_path + folder_name + "_ok"):
        print("o arquivo ja estava previamente pronto")
        return
    elif not os.path.exists(work_path + folder_name):
        os.mkdir(work_path + folder_name)

    vendas = fs.sort_file(imput_path + file_path, hour)
    index = fs.get_daterange(imput_path + file_path, hour, freq)
    print(index)

    for date in index:
        global_dict[str(date).split(" ")[0]] = {}
    aux_vendas = []
    week = ''

    for venda in vendas:
        if hour:
            dia, mes, ano = venda.split(";")[1].split(" ")[0].split("/")
        else:
            dia, mes, ano = venda.split(";")[1].split("/")
        date_now = ano + "-" + mes + "-" + dia

        if date_now in global_dict.keys():
            if date_now != week and week != '':
                fs.file_constructor(aux_vendas, week, work_path + folder_name)
                aux_vendas.clear()
            week = date_now
        if week in index:
            aux_vendas.append(fs.transaction_handler(venda, week, global_dict, byitens=by_itens, bygroup=by_group, bysubsection=by_subsections, itens_to_concat=itens_to_concat))

    for filename in os.listdir(work_path + folder_name):
        os.rename(work_path + folder_name + "/" + filename, work_path + folder_name + "/" + filename.split("_")[0] + ".pkl")

    with open(work_path + folder_name + "/" + "globaldict.pickle", 'wb') as handle:
        pickle.dump(global_dict, handle, protocol=pickle.HIGHEST_PROTOCOL)

    os.rename(work_path + folder_name, work_path + folder_name + "_ok")

    return
