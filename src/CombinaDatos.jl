#

using Pkg,Printf, CSV, JuliaDB, Distributions, OnlineStats, DataFrames, Dates, Plots, PyCall, HTTP, ZipFile, Logging, TerminalLoggers, Dates, XLSX, ExcelReaders
using CSVFiles

function framesDatos(rutaArchivo1, rutaArchivo2)
    dataFrameArchivo1 = DataFrames
    dataFrameArchivo2 = DataFrames
    if rutaArchivo1 == rutaArchivo2
        return "MismoArchivo"
    else
        if !contains(rutaArchivo1,".csv")
            intensidadMigratoriaXLS = readxlsheet(rutaArchivo1,"IIM2010_BASEMUN")
            intensidadMigratoriaDataFrame = DataFrame(  ENT=Int64.(intensidadMigratoriaXLS[2:length(intensidadMigratoriaXLS[:,1]),1]),
                                                        NOM_ENT=String.(intensidadMigratoriaXLS[2:length(intensidadMigratoriaXLS[:,2]),2]),
                                                        MUN=Int64.(intensidadMigratoriaXLS[2:length(intensidadMigratoriaXLS[:,3]),3]),
                                                        NOM_MUN=String.(intensidadMigratoriaXLS[2:length(intensidadMigratoriaXLS[:,4]),4]),
                                                        IIM_2010=Float64.(intensidadMigratoriaXLS[2:length(intensidadMigratoriaXLS[:,10]),10]))
            nuevosNombresintensidadMigratoriaDataFrame = ["claveEntidad","Entidad","claveMunicipio","Municipio","IntensidadMigratoria2010"]
            dataFrameArchivo1 = DataFrames.rename!(intensidadMigratoriaDataFrame,Symbol.(nuevosNombresintensidadMigratoriaDataFrame))
        else
            if contains(rutaArchivo1,"tec_nac_proyecciones")
                natalidad = CSV.read(rutaArchivo1)
                nuevosNombresNatalidad = ["Renglon","Anio","Entidad","claveEntidad","grupoEdad","tasaNatalidad","Nacimientos"]
                dataFrameArchivo1 = DataFrames.rename!(natalidad,Symbol.(nuevosNombresNatalidad))
            elseif contains(rutaArchivo1,"def_edad_proyecciones")
                defunciones = CSV.read(rutaArchivo1)
                nuevosNombresDefunciones = ["Regnlon","Anio","Entidad","claveEntidad","Sexo","Edad","Defunciones"]
                dataFrameArchivo1 = DataFrames.rename!(defunciones,Symbol.(nuevosNombresDefunciones))
            elseif contains(rutaArchivo1,"COVID19MEXICO")
                datosCovid = CSV.read(rutaArchivo1)
                nuevosNombresDatosCovid = ["fechaActualizacion","idRegistro","Origen","Sector","EntidadUM","Sexo","claveEntidad","EntidadRES","MunicipioRES",
                                           "tipoPaciente","fechaIngreso","fechaSintomas","fechaDefuncion","Intubado","Neumonia","Edad","Nacionalidad","Embarazo",
                                           "hablaIndigena","Diabetes","Epoc","Asma","Inmunosupresores","Hipertension","otraComplicacion","Cardiovascular",
                                           "Obesidad","RenalCronica","Tabaquismo","otroCaso","Resultado","Migrante","PaisNacionalidad","PaisOrigen","UCI"]
                dataFrameArchivo1 = DataFrames.rename!(datosCovid,nuevosNombresDatosCovid)
            end
        end
        if !contains(rutaArchivo2,".csv")
            intensidadMigratoriaXLS = readxlsheet(rutaArchivo2,"IIM2010_BASEMUN")
            intensidadMigratoriaDataFrame = DataFrame(  ENT=Int64.(intensidadMigratoriaXLS[2:length(intensidadMigratoriaXLS[:,1]),1]),
                                                        NOM_ENT=String.(intensidadMigratoriaXLS[2:length(intensidadMigratoriaXLS[:,2]),2]),
                                                        MUN=Int64.(intensidadMigratoriaXLS[2:length(intensidadMigratoriaXLS[:,3]),3]),
                                                        NOM_MUN=String.(intensidadMigratoriaXLS[2:length(intensidadMigratoriaXLS[:,4]),4]),
                                                        IIM_2010=Float64.(intensidadMigratoriaXLS[2:length(intensidadMigratoriaXLS[:,10]),10]))
            nuevosNombresintensidadMigratoriaDataFrame = ["claveEntidad","Entidad","claveMunicipio","Municipio","IntensidadMigratoria2010"]
            dataFrameArchivo2 = DataFrames.rename!(intensidadMigratoriaDataFrame,Symbol.(nuevosNombresintensidadMigratoriaDataFrame))
        else
            if contains(rutaArchivo2,"tef_nac_proyecciones")
                natalidad = CSV.read(rutaArchivo2)
                nuevosNombresNatalidad = ["Renglon","Anio","Entidad","claveEntidad","grupoEdad","tasaNatalidad","Nacimientos"]
                dataFrameArchivo2 = DataFrames.rename!(natalidad,Symbol.(nuevosNombresNatalidad))
            elseif contains(rutaArchivo2,"def_edad_proyecciones")
                defunciones = CSV.read(rutaArchivo2)
                nuevosNombresDefunciones = ["Renglon","Anio","Entidad","claveEntidad","Sexo","Edad","Defunciones"]
                dataFrameArchivo2 = DataFrames.rename!(defunciones,Symbol.(nuevosNombresDefunciones))
            elseif contains(rutaArchivo2,"COVID19MEXICO")
                datosCovid = CSV.read(rutaArchivo2)
                nuevosNombresDatosCovid = ["fechaActualizacion","idRegistro","Origen","Sector","EntidadUM","Sexo","claveEntidad","EntidadRES","MunicipioRES",
                                           "tipoPaciente","fechaIngreso","fechaSintomas","fechaDefuncion","Intubado","Neumonia","Edad","Nacionalidad","Embarazo",
                                           "hablaIndigena","Indigena","Diabetes","Epoc","Asma","Inmunosupresores","Hipertension","otraComplicacion","Cardiovascular",
                                           "Obesidad","RenalCronica","Tabaquismo","otroCaso","muestraLaboratorio","ResultadoLaboratorio","MuestraAntigeno",
                                           "ResultadoAntigeno","ClasificacionFinal","Migrante","PaisNacionalidad","PaisOrigen","UCI"]
                dataFrameArchivo2 = DataFrames.rename!(datosCovid,nuevosNombresDatosCovid)
            end
        end
    end
    dataFrames = [dataFrameArchivo1,dataFrameArchivo2]
    return dataFrames
end

function combinaDatos(frame1,frame2)
    if isempty(frame1)
        return "Frame 1 Vacio"
    elseif isempty(frame2)
        return "Frame 2 Vacio"
    else
        framesCombinados = DataFrames.join(frame1,frame2, kind= :outer, on = intersect(names(frame1),names(frame2)))
        return framesCombinados
    end
end

function exportaDatos(rutaDestinoArchivo,nombreArchivo,frame)
    if isempty(rutaDestinoArchivo)
        return "Ingrese Ruta de Archivo"
    elseif isempty(nombreArchivo)
        return "Ingrese el Nombre del Archivo Resultante"
    elseif isempty(frame)
        return "Falta el Frame a Exportar"
    else
        rutaFinal = rutaDestinoArchivo * nombreArchivo
        CSVFiles.save(rutaFinal,frame)
        return
    end
end
