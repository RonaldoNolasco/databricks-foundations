-- abriendo el aula
%run ./Includes/Classroom-Setup

-- Corriendo el lenguaje por defecto (python)
print("Run default language")

-- Usando diversos lenguajes (con los magic words)
%python
print("Run python")

-- Corriendo scala
%scala
println("Run scala")

-- Corriendo sql
%sql
select "Run SQL"

-- Corriendo R
%r
print("Run R", quote=FALSE)

-- Usando comandos bash
%sh ps | grep 'java'

-- Renderizando html
html = """<h1 style="color:orange;text-align:center;font-family:Courier">Render HTML</h1>"""
displayHTML(html)

-- Renderizando markdown
%md
# Heading 1
### Heading 3
> block quote

1. **bold**
2. *italicized*
3. ~~strikethrough~~

-- Listando archivos del DBFS (databricks file system)
%fs ls

-- listando directorios
%fs ls /databricks-datasets

-- leyendo archivos (head)
%fs head /databricks-datasets/README.md

-- viendo los discos montados
%fs mounts

-- ayuda del filesystem
%fs help

-- corriendo comandos con el dbutils
dbutils.fs.ls("/databricks-datasets")

-- usando display para mostrars los archivos
files = dbutils.fs.ls("/databricks-datasets")
display(files)

-- creando tabla desde un archivo parquet
%sql
CREATE TABLE IF NOT EXISTS events USING parquet OPTIONS (path "/mnt/training/ecommerce/events/events.parquet");

-- mostrando el nombre de la bd actual
print(databaseName)

-- usando sentencias sql en spark
%sql
SELECT * FROM events

-- mas sentencias sql
%sql
SELECT traffic_source, SUM(ecommerce.purchase_revenue_in_usd) AS total_revenue
FROM events
GROUP BY traffic_source

-- creando un widget
%sql
CREATE WIDGET TEXT state DEFAULT "CA"

-- usando como argumentos el valor del widget
%sql
SELECT *
FROM events
WHERE geo.state = getArgument("state")

-- removiendo widget
%sql
REMOVE WIDGET state

-- creando widget de texto y selector
dbutils.widgets.text("name", "Brickster", "Name")
dbutils.widgets.multiselect("colors", "orange", ["red", "orange", "black", "blue"], "Traffic Sources")

-- usando los datos de los widget
name = dbutils.widgets.get("name")
colors = dbutils.widgets.get("colors").split(",")

html = "<div>Hi {}! Select your color preference.</div>".format(name)
for c in colors:
  html += """<label for="{}" style="color:{}"><input type="radio"> {}</label><br>""".format(c, c, c)

displayHTML(html)

-- removiendo todos los widgets
dbutils.widgets.removeAll()
