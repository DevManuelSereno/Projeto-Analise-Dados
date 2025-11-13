from dbfread import DBF

table = DBF(r"C:\Users\User\Desktop\Dados\dadosdbf\RDBA1901.dbf", encoding="latin1")

print("Total de colunas:", len(table.fields))
for f in table.fields:
    print(f.name)