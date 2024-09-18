router.get("/fichas/consulta_ficha_otros_cargos/:cedula/:cod_dep", async (req, res) => {
const { cedula, cod_dep } = req.params;

if (!cedula) {
res.status(404).send("Cedula requerida");
return false;
}

const getCondition = () => {
SELECT \*
FROM cnmd08_historia_trabajador as cht
INNER JOIN v_cnmd06_fichas_2 as vcf ON cht.cedula_identidad=vcf.cedula_identidad and cht.cod_dep=vcf.cod_dep and cht.cod_tipo_nomina=vcf.cod_tipo_nomina
and cht.cod_cargo=vcf.cod_cargo and vcf.cod_ficha=cht.cod_ficha
where cht.cedula_identidad=19760800

    if (cod_dep.includes("-")) {
      const codSplit = cod_dep.split("-");
      if (codSplit.length < 2) {
        return null;
      }
      if (codSplit[1] != "00") {
        return `f.cod_dep=1 and f.cod_secretaria=${codSplit[0]} and f.cod_direccion=${codSplit[1]}`;
      }
      if (codSplit[0] == "01") {
        return `f.cod_dep=1 and f.cod_secretaria=${codSplit[0]} and f.cod_direccion NOT IN (2,3,4,5)`;
      }
      if (codSplit[0] == "10") {
        return `f.cod_dep=1 and f.cod_secretaria=${codSplit[0]} and f.cod_direccion NOT IN (8,9)`;
      }
      if (codSplit[0] == "13") {
        return `f.cod_dep=1 and f.cod_secretaria=${codSplit[0]} and f.cod_direccion NOT IN (2,3,4,5,8)`;
      }
      if (codSplit[0] == "15") {
        return `f.cod_dep=1 and f.cod_secretaria=${codSplit[0]} and f.cod_direccion NOT IN (1,2,3,4,5,8,9)`;
      }

      return `f.cod_dep=1 and f.cod_secretaria=${codSplit[0]}`;
    }
    if (cod_dep < 1000 || cod_dep == 1024 || cod_dep == 1025) {
      return null;
    }

    return `f.cod_dep=${cod_dep}`;

};

try {
let valid = false;
let db = 1;
let beneficiario = "";

    const sqlQuery =
      cod_dep == "1"
        ? `SELECT

fb.cedula_identidad, dp.primer_nombre || ' ' || dp.segundo_nombre || ' ' || dp.primer_apellido || ' ' || dp.segundo_apellido as nombre,
fb.cod_dep,(SELECT denominacion from arrd05 where cod_dep=fb.cod_dep) as dependencia, fb.cod_tipo_nomina, hn.denominacion, fb.cod_cargo, fb.cod_ficha, f.condicion_actividad, h.cod_puesto, f.fecha_ingreso, f.fecha_movimiento,
( SELECT devolver_denominacion_puesto(( SELECT xy.clasificacion_personal FROM cnmd01 xy
WHERE xy.cod_dep = fb.cod_dep AND xy.cod_tipo_nomina = fb.cod_tipo_nomina), h.cod_puesto) AS devolver_denominacion_puesto) AS demonimacion_puesto
FROM (SELECT cod_dep, cod_tipo_nomina,
cod_cargo, cod_ficha, cedula_identidad
FROM cnmd08_historia_trabajador
where ano>=2021 and cod_dep!=${cod_dep} and cedula_identidad=${cedula}
group by cod_dep, cod_tipo_nomina,
cod_cargo, cod_ficha, cedula_identidad) as fb
LEFT JOIN cnmd06_fichas as f ON f.cod_ficha=fb.cod_ficha and f.cod_cargo=fb.cod_cargo and f.cedula_identidad=fb.cedula_identidad
LEFT JOIN cnmd05 as h ON h.cod_dep=fb.cod_dep and h.cod_tipo_nomina=fb.cod_tipo_nomina and h.cod_cargo=fb.cod_cargo
FULL OUTER JOIN cnmd01 as hn on hn.cod_dep=fb.cod_dep and hn.cod_tipo_nomina=fb.cod_tipo_nomina
FULL OUTER JOIN cnmd06_datos_personales as dp on dp.cedula_identidad=fb.cedula_identidad
where h.cod_puesto>0 and f.condicion_actividad not in (1) and fb.cedula_identidad =${cedula} [condition_ext]`
        : `SELECT 
 fb.cedula_identidad, dp.primer_nombre || ' ' || dp.segundo_nombre || ' ' || dp.primer_apellido || ' ' || dp.segundo_apellido as nombre, 
    fb.cod_dep,(SELECT denominacion from arrd05 where cod_dep=fb.cod_dep) as dependencia, fb.cod_tipo_nomina, hn.denominacion, fb.cod_cargo, fb.cod_ficha, f.condicion_actividad, h.cod_puesto, f.fecha_ingreso, f.fecha_movimiento,
    ( SELECT devolver_denominacion_puesto(( SELECT xy.clasificacion_personal FROM cnmd01 xy
      WHERE xy.cod_dep = fb.cod_dep AND xy.cod_tipo_nomina = fb.cod_tipo_nomina), h.cod_puesto) AS devolver_denominacion_puesto) AS demonimacion_puesto
    FROM (SELECT cod_dep, cod_tipo_nomina,
       cod_cargo, cod_ficha, cedula_identidad
  FROM cnmd08_historia_trabajador
  where cod_dep!=${cod_dep} and cedula_identidad=${cedula}
  group by cod_dep, cod_tipo_nomina, 
       cod_cargo, cod_ficha, cedula_identidad) as fb
    LEFT JOIN cnmd06_fichas as f ON f.cod_ficha=fb.cod_ficha and f.cod_cargo=fb.cod_cargo and f.cedula_identidad=fb.cedula_identidad
    LEFT JOIN cnmd05 as h ON h.cod_dep=fb.cod_dep and h.cod_tipo_nomina=fb.cod_tipo_nomina and h.cod_cargo=fb.cod_cargo
    FULL OUTER JOIN cnmd01 as hn on hn.cod_dep=fb.cod_dep and hn.cod_tipo_nomina=fb.cod_tipo_nomina
    FULL OUTER JOIN cnmd06_datos_personales as dp on dp.cedula_identidad=fb.cedula_identidad 
    where h.cod_puesto>0 and f.condicion_actividad not in (1) and fb.cedula_identidad =${cedula} [condition_ext]`;

    const query = await unifiedQuery({ sqlQuery, table: "fb." });

    if (query.length > 0) {
      res.json(query);
      return true;
    } else {
      res.status(404).send({ message: "No existe resultados", error: "" });
      return false;
    }

} catch (error) {
res.status(500).send({ message: "Error en la consulta unificada", error: error.message });
}
});

router.get("/fichas/consulta_ficha_cargos/:cedula/:cod_dep", async (req, res) => {
const { cedula, cod_dep } = req.params;

if (!cedula) {
res.status(404).send("Cedula requerida");
return false;
}
const getConditionDep = () => {
if (cod_dep.includes("-")) {
return `ano>=2021 and cod_dep=1`;
}

    return `cod_dep=${cod_dep}`;

};

try {
const sqlQuery = `SELECT 
 fb.cedula_identidad, dp.primer_nombre || ' ' || dp.segundo_nombre || ' ' || dp.primer_apellido || ' ' || dp.segundo_apellido as nombre, 
    fb.cod_dep,(SELECT denominacion from arrd05 where cod_dep=fb.cod_dep) as dependencia, fb.cod_tipo_nomina, hn.denominacion, fb.cod_cargo, fb.cod_ficha, f.condicion_actividad, h.cod_puesto, f.fecha_ingreso, f.fecha_movimiento,
    ( SELECT devolver_denominacion_puesto(( SELECT xy.clasificacion_personal FROM cnmd01 xy
      WHERE xy.cod_dep = fb.cod_dep AND xy.cod_tipo_nomina = fb.cod_tipo_nomina), h.cod_puesto) AS devolver_denominacion_puesto) AS demonimacion_puesto
    FROM (SELECT cod_dep, cod_tipo_nomina,
       cod_cargo, cod_ficha, cedula_identidad
  FROM cnmd08_historia_trabajador
  where ${getConditionDep()} and cedula_identidad=${cedula}
  group by cod_dep, cod_tipo_nomina, 
       cod_cargo, cod_ficha, cedula_identidad) as fb
    LEFT JOIN cnmd06_fichas as f ON f.cod_ficha=fb.cod_ficha and f.cod_cargo=fb.cod_cargo and f.cedula_identidad=fb.cedula_identidad
    LEFT JOIN cnmd05 as h ON h.cod_dep=fb.cod_dep and h.cod_tipo_nomina=fb.cod_tipo_nomina and h.cod_cargo=fb.cod_cargo
    FULL OUTER JOIN cnmd01 as hn on hn.cod_dep=fb.cod_dep and hn.cod_tipo_nomina=fb.cod_tipo_nomina
    FULL OUTER JOIN cnmd06_datos_personales as dp on dp.cedula_identidad=fb.cedula_identidad 
    where h.cod_puesto>0 and f.condicion_actividad not in (1) and fb.cedula_identidad =${cedula} [condition_ext]`;

    const query = await unifiedQuery({ sqlQuery, table: "fb." });

    if (query.length > 0) {
      res.json(query);
      return true;
    } else {
      res.status(404).send({ message: "No existe resultados", error: "" });
      return false;
    }

} catch (error) {
res.status(500).send({ message: "Error en la consulta unificada", error: error.message });
}
});

router.get("/fichas/consulta_ficha_otros_experiencias_administrativas/:cedula", async (req, res) => {
const { cedula, cod_dep } = req.params;

if (!cedula) {
res.status(404).send("Cedula requerida");
return false;
}

try {
const sqlQuery = `SELECT 
 cargo_desempenado,
  entidad_federal
  FROM cnmd06_datos_experiencia_administrativa
  where cedula=${cedula}`;

    const query = await unifiedQuery({ sqlQuery });

    var list = [];
    query.forEach((element) => {
      if (!list.some((e) => e.cargo_desempenado === element.cargo_desempenado && e.entidad_federal === element.entidad_federal)) {
        list.push(element);
      }
    });
    res.json(list);
    return true;

} catch (error) {
res.status(500).send({ message: "Error en la consulta unificada", error: error.message });
}
});
