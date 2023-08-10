TRUNCATE TABLE `myproject-387020.results_zone.clientes`;
INSERT INTO `myproject-387020.results_zone.clientes`
SELECT
  CAST(id AS int) AS id,
  Nombre AS nombre,
  Pais AS pais,
  CAST(edad AS INT) AS edad,
  NULLIF(Ocupacion,"") AS ocupacion,
  CAST(NULLIF(Score,"") AS int) AS score,
  CAST(NULLIF(REGEXP_REPLACE(Salario_net_USD,"[^0-9]",""),"") AS int) AS salario_net_USD,
  NULLIF(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(Estado_Civil),".*So\\w*", "Soltero"), ".*C\\ws\\w*", "Casado"),"") AS estado_civil,
  CAST(Estado AS int) AS estado,
  Fecha_Inactividad AS fecha_inactividad,
  regexp_replace(REGEXP_REPLACE(Genero,"[Mm]+", "M"),"[Ff]+", "F") as genero,
  Device AS device,
  Nivel_Educativo AS nivel_educativo,
  Carrera AS carrera
FROM
  `myproject-387020.raw_zone.clientes`;

TRUNCATE TABLE `myproject-387020.results_zone.compras`;
INSERT INTO `myproject-387020.results_zone.compras`
SELECT  
  CAST(id as int) as id,
  CAST(cust_id as int) as cust_id,
  CAST(prod_id as int) as prod_id,
  CAST(Gasto as int) as gasto,
  CAST(FechaCompra as DATE FORMAT "DD/MM/YYYY") as fecha_compra,
  Mediopago as medio_pago
FROM `myproject-387020.raw_zone.compras`;

TRUNCATE TABLE `myproject-387020.results_zone.productos`;
INSERT INTO `myproject-387020.results_zone.productos`
SELECT
  CAST(id AS INT) AS id,
  nombre,
  CAST(ValorUSD AS INT) AS valor_USD,
  CAST(Cantidad_Datos_MB AS INT) AS cantidad_datos_MB,
  CAST(Vigencia AS INT) AS vigencia,
  Telefonia as telefonia
FROM `myproject-387020.raw_zone.productos`;

TRUNCATE TABLE `myproject-387020.results_zone.unificada`;
INSERT INTO `myproject-387020.results_zone.unificada`
SELECT
 t1.id	
,t1.cust_id
,t1.prod_id
,t1.Gasto
,t1.fecha_compra
,t1.Medio_pago
--,t2.id
,t2.nombre as cust_nombre
,t2.pais
,t2.edad
,t2.ocupacion
,t2.score
,t2.salario_net_usd
,t2.estado_civil
,t2.estado
,t2.Fecha_inactividad	
,t2.genero
,t2.device
,t2.nivel_educativo
,t2.carrera
--,t3.id
,t3.nombre as prod_nombre
,t3.valor_USD
,t3.cantidad_datos_MB
,t3.vigencia
,t3.telefonia
,t4.coordenadas
FROM `myproject-387020.results_zone.compras` t1
LEFT JOIN `myproject-387020.results_zone.clientes` t2
      ON t1.cust_id = t2.id
LEFT JOIN `myproject-387020.results_zone.productos` t3
    ON t1.prod_id = t3.id
LEFT JOIN `myproject-387020.results_zone.paises` t4
      ON t2.pais = t4.pais
;