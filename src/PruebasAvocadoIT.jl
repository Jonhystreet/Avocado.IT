
function Instalacion()
     Pkg.add("CSV")
     Pkg.add("ExcelReaders")
     Pkg.add(PackageSpec(url="https://github.com/JuliaData/JuliaDB.jl"))
     Pkg.add("Distributions")
     Pkg.add("OnlineStats")
     Pkg.add("DataFrames")
     Pkg.add("Dates")
     Pkg.add("Plots")
     Pkg.add("PyCall")
     Pkg.build("PyCall")
     Pkg.add("HTTP")
     Pkg.add("ZipFile")
     Pkg.add("Logging")
     Pkg.add("TerminalLoggers")
     Pkg.add("Dates")
     Pkg.add("XLSX")
     Pkg.add("CSVFiles")
end
using Pkg
Instalacion()

using CSVFiles,Printf, CSV, JuliaDB, Distributions, OnlineStats, DataFrames, Dates, Plots, PyCall, HTTP, ZipFile, Logging, TerminalLoggers, Dates, XLSX, ExcelReaders
#Barra de progreso
global_logger(TerminalLogger(right_justify=120))

#Modulo para descomprimir archivos de python
zipfile = pyimport("zipfile")

struct Descargable
    path::String
    fecha::String
end

struct Descargados
    covid::Descargable
    coneval::Descargable
    im::Descargable
    defunciones::Descargable
    natalidad::Descargable
end

struct TablasIM
    uno::DataFrame
    dos::DataFrame
end

#Regresa string URL de descarga
#links_covid(FECHA)
function links_Covid(f="")
    ff = DateFormat("dd-mm-yyyy")
    fe = Date(f,ff)
    if string(Dates.year(fe)) == "2020"
        return covidLink = string("http://datosabiertos.salud.gob.mx/gobmx/salud/datos_abiertos/historicos/",
            @sprintf("%02d",Dates.month(fe)),"/datos_abiertos_covid19_",
            @sprintf("%02d",Dates.day(fe)),".",
            @sprintf("%02d",Dates.month(fe)),".2020.zip")
    elseif string(Dates.year(fe)) == "2021"
        return covidLink = string("http://datosabiertos.salud.gob.mx/gobmx/salud/datos_abiertos/historicos/2021/",
            @sprintf("%02d",Dates.month(fe)),"/datos_abiertos_covid19_",
            @sprintf("%02d",Dates.day(fe)),".",
            @sprintf("%02d",Dates.month(fe)),".2021.zip")
    end
end

#Descomprime archivo, regresa la direccion del archivo
#unzip(ARCHIVO_COMPRIMIDO, DIRECCION_DESCOMPRIMIR)
function unzip(rar,pathS="")
    local pathC = ""
    rzip = zipfile.ZipFile(rar)
    for i in rzip.namelist()
        path = joinpath(pathS,i)
        if isempty(pathC)
                pathC = path
        end
        rzip.extract(i, pathS)
    end
    rzip.close()
    rm(rar, force=true, recursive=true)
    return pathC
end

covidActual = "http://datosabiertos.salud.gob.mx/gobmx/salud/datos_abiertos/datos_abiertos_covid19.zip"
#Descarga archivo covid, regresa la dirrecion del archivo
#descargar_covid(DIRECCION,FECHA), si no se especifica DIRECCION el archivo se guarda en la carpeta actual, si no se especifica FECHA se obtienen los datos mas actualizados
#FECHA formato dia-mes-año ejem."15-1-2020"
function descargar_covid(path="",f="")
    if isempty(path)
        path = pwd()
    end
    if isempty(f)
        covidLink = covidActual
        f = string(today())
    else
        covidLink = links_Covid(f)
    end
    pathC = joinpath(path,"covid")
    rm(pathC, force=true, recursive=true)
    mkdir(pathC)
    rar = HTTP.download(covidLink, pathC)
    return Descargable(unzip(rar, pathC),f)
end

#Datos de CONEVAL 2008 al 2018
conevalLink = "https://www.coneval.org.mx/Medicion/MP/Documents/Pobreza_18/AE_nacional_estatal_2008_2018.zip"

#Descarga los datos relevantes del CONEVAL, regresa direccion del archivo
#descargar_coneval(DIRECCION), si no se especifica DIRECCION el archivo se guarda en la carpeta actual
function descargar_coneval(path="",f="")
    if isempty(path)
        path = pwd()
    end
    if isempty(f)
        f = string(today())
    end
    pathC = joinpath(path,"coneval")
    rm(pathC, force=true, recursive=true)
    mkdir(pathC)
    rar = HTTP.download(conevalLink, pathC)
    return Descargable(unzip(rar, pathC),f)
end

#Datos intensidad migratoria por municipios, 2010
imLink = "http://www.omi.gob.mx/work/models/OMI/Resource/538/1/images/IIM2010_BASEMUN.xls"

#Descarga los datos del Observatorio de Migración Internacional, regresa direccion del archivo
#descargar_im(DIRECCION), si no se especifica DIRECCION el archivo se guarda en la carpeta actual
function descargar_im(path="",f="")
    if isempty(path)
        path = pwd()
    end
    if isempty(f)
        f = string(today())
    end
    pathC = joinpath(path,"Intensidad Migratoria")
    rm(pathC, force=true, recursive=true)
    mkdir(pathC)
    rar = HTTP.download(imLink, pathC)
    return Descargable(rar,f)
end
#Descarga los datos de la fecundacion y nacimientos a nivel nacional en mexico

natalidad_dat= "http://www.conapo.gob.mx/work/models/CONAPO/Datos_Abiertos/Proyecciones2018/tef_nac_proyecciones.csv"

function descargar_natalidad(path="",f="")
    if isempty(path)
        path = pwd()
    end
    if isempty(f)
        f = string(today())
    end
    pathC = joinpath(path,"Natalidad")
    rm(pathC, force=true, recursive=true)
    mkdir(pathC)
    rar = HTTP.download(natalidad_dat, pathC)
    return Descargable(rar,f)
end

#descargar_defunciones
deflink = "http://www.conapo.gob.mx/work/models/CONAPO/Datos_Abiertos/Proyecciones2018/def_edad_proyecciones_n.csv"
#Descarga los datos relevantes del conapo, regresa direccion del archivo
#descargar_defunciones(DIRECCION), si no se especifica DIRECCION el archivo se guarda en la carpeta actual
function descargar_defunciones(path="",f="")
    if isempty(path)
        path = pwd()
    end
    if isempty(f)
        f = string(today())
    end
    pathC = joinpath(path,"Defunciones")
    rm(pathC, force=true, recursive=true)
    mkdir(pathC)
    rar = HTTP.download(deflink, pathC)
    return Descargable(rar,f)
end

#Combina las funciones de descarga
#descargar_datos(DIRECCION,FECHA), si no se especifica DIRECCION se ocupa la carpeta actual
function descargar_datos(path="",f="")
    path= ""
    f=""
    if isempty(path)
        path = pwd()
    end
    pathC = joinpath(path,"Descargables")
    rm(pathC, force=true, recursive=true)
    mkdir(pathC)
    uno = descargar_covid(pathC,f)
    dos = descargar_coneval(pathC,f)
    tres = descargar_im(pathC,f)
    cuatro = descargar_defunciones(pathC,f)
    cinco = descargar_natalidad(pathC,f)
    return Descargados(uno, dos, tres, cuatro ,cinco)
end

#Regresa dos DataFrames usando datos del Indice Migratorio
function datos_im(pd::Descargados)
    imt = readxlsheet(pd.im.path, "IIM2010_BASEMUN")
    df= DataFrame(ENT=Int64.(imt[2:length(imt[:,1]),1]),
        NOM_ENT=String.(imt[2:length(imt[:,2]),2]),
        MUN=Int64.(imt[2:length(imt[:,3]),3]),
        NOM_MUN=String.(imt[2:length(imt[:,4]),4]),
        IIM_2010=Float64.(imt[2:length(imt[:,10]),10]))

    mnum = Any[]
    for i in minimum(df.ENT):maximum(df.ENT)
        push!(mnum, (length(df[(df.ENT .== i),:][:,1])))
    end

    improm = Any[]
    for i in minimum(df.ENT):maximum(df.ENT)
        push!(improm,(sum(df[(df.ENT .== i),5])/mnum[i]))
    end

    df2=DataFrame(Entidad = unique(df.NOM_ENT), NMUN=mnum, IM=improm)
    return TablasIM(df,df2)
end

hola = descargar_datos()

#Cruce de Datos

datosCovid = CSV.read(hola.covid.path)
intensidadMigratoriaXLS = readxlsheet("D:\\Documentos\\Pruebas\\Descargables\\Intensidad Migratoria\\IIM2010_BASEMUN.xls","IIM2010_BASEMUN")
intensidadMigratoriaDataFrame = DataFrame(  ENT=Int64.(intensidadMigratoriaXLS[2:length(intensidadMigratoriaXLS[:,1]),1]),
                                            NOM_ENT=String.(intensidadMigratoriaXLS[2:length(intensidadMigratoriaXLS[:,2]),2]),
                                            MUN=Int64.(intensidadMigratoriaXLS[2:length(intensidadMigratoriaXLS[:,3]),3]),
                                            NOM_MUN=String.(intensidadMigratoriaXLS[2:length(intensidadMigratoriaXLS[:,4]),4]),
                                            IIM_2010=Float64.(intensidadMigratoriaXLS[2:length(intensidadMigratoriaXLS[:,10]),10]))
defunciones = CSV.read(hola.defunciones.path)
natalidad = CSV.read(hola.natalidad.path)

#=nuevosNombresDatosCovid = ["fechaActualizacion","idRegistro","Origen","Sector","EntidadUM","Sexo","claveEntidad"]=#
nuevosNombresintensidadMigratoriaDataFrame = ["claveEntidad","Entidad","claveMunicipio","Municipio","IntensidadMigratoria2010"]
nuevosNombresNatalidad = ["Renglon","Anio","Entidad","claveEntidad","grupoEdad","tasaNatalidad","Nacimientos"]

intensidadMigratoriaDataFrame
datosCovid = DataFrame(datosCovid)
defunciones = DataFrame(defunciones)
natalidad = DataFrame(natalidad)

DataFrames.rename!(intensidadMigratoriaDataFrame,Symbol.(nuevosNombresintensidadMigratoriaDataFrame))
DataFrames.rename!(natalidad,Symbol.(nuevosNombresNatalidad))
Data

intensidadMigratoriaDataFrameXnatalidad = DataFrames.join(intensidadMigratoriaDataFrame, natalidad, kind= :outer, on = intersect(names(natalidad),names(intensidadMigratoriaDataFrame)))
intensidadMigratoriaDataFrameXnatalidad

CSVFiles.save("C:\\Users\\chris\\Desktop\\Cruce_de_Info.csv",intensidadMigratoriaDataFrameXnatalidad)
