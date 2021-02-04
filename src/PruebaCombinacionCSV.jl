#

using Pkg,Printf, CSV, JuliaDB, Distributions, OnlineStats, DataFrames, Dates, Plots, PyCall, HTTP, ZipFile, Logging, TerminalLoggers, Dates, XLSX, ExcelReaders
using CSVFiles
datosCovid = CSV.read("D:\\Documentos\\Pruebas\\Descargables\\covid\\200813COVID19MEXICO.csv")
intensidadMigratoriaXLS = readxlsheet("D:\\Documentos\\Pruebas\\Descargables\\Intensidad Migratoria\\IIM2010_BASEMUN.xls","IIM2010_BASEMUN")
intensidadMigratoriaDataFrame = DataFrame(  ENT=Int64.(intensidadMigratoriaXLS[2:length(intensidadMigratoriaXLS[:,1]),1]),
                                            NOM_ENT=String.(intensidadMigratoriaXLS[2:length(intensidadMigratoriaXLS[:,2]),2]),
                                            MUN=Int64.(intensidadMigratoriaXLS[2:length(intensidadMigratoriaXLS[:,3]),3]),
                                            NOM_MUN=String.(intensidadMigratoriaXLS[2:length(intensidadMigratoriaXLS[:,4]),4]),
                                            IIM_2010=Float64.(intensidadMigratoriaXLS[2:length(intensidadMigratoriaXLS[:,10]),10]))
defunciones = CSV.read("D:\\Julia\\Programas\\Descargables\\Defunciones\\def_edad_proyecciones_n.csv")
natalidad = CSV.read("D:\\Julia\\Programas\\Descargables\\Natalidad\\tef_nac_proyecciones.csv")

#=nuevosNombresDatosCovid = ["fechaActualizacion","idRegistro","Origen","Sector","EntidadUM","Sexo","claveEntidad"]=#
nuevosNombresintensidadMigratoriaDataFrame = ["claveEntidad","Entidad","claveMunicipio","Municipio","IntensidadMigratoria2010"]
nuevosNombresNatalidad = ["Renglon","Anio","Entidad","claveEntidad","grupoEdad","tasaNatalidad","Nacimientos"]
nuevosNombresDefunciones = ["#"]


intensidadMigratoriaDataFrame
datosCovid = DataFrame(datosCovid)
defunciones = DataFrame(defunciones)
natalidad = DataFrame(natalidad)

DataFrames.rename!(intensidadMigratoriaDataFrame,Symbol.(nuevosNombresintensidadMigratoriaDataFrame))
DataFrames.rename!(natalidad,Symbol.(nuevosNombresNatalidad))

intensidadMigratoriaDataFrameXnatalidad = DataFrames.join(intensidadMigratoriaDataFrame, natalidad, kind= :outer, on = intersect(names(natalidad),names(intensidadMigratoriaDataFrame)))
intensidadMigratoriaDataFrameXnatalidad

CSVFiles.save("C:\\Users\\chris\\Desktop\\Cruce_de_Info.csv",intensidadMigratoriaDataFrameXnatalidad)
