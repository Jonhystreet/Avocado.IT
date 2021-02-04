#

include("AvocadoIT.jl")

#AvocadoIT.instalacion()
AvocadoIT.descargar_datos("C:\\Users\\chris\\Desktop","24-09-2020")



include("AvocadoIT.jl")
dataFrames = AvocadoIT.framesDatos("C:\\Users\\chris\\Desktop\\Descargables\\covid\\200924COVID19MEXICO.csv",
                                    "D:\\Julia\\Programas\\Descargables\\Natalidad\\tef_nac_proyecciones.csv")

dataFrames[1]
dataFrames[2]

framesCombinados = AvocadoIT.combinaDatos(dataFrames[1],dataFrames[2])
framesCombinados

AvocadoIT.exportaDatos("C:\\Users\\chris\\Desktop\\","Cruce_de_Info_XD.csv",framesCombinados)
