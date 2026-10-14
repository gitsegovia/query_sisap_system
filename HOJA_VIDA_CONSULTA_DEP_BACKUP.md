# Respaldo: `GET /hoja_vida/consulta_dep/:cod_dep` (antes de los ajustes)

Estado del endpoint en el commit `f426ce3` (`src/routes.js`), previo al alineamiento con `/sisap/empleado`.

## Reversión rápida

Antes de hacer commit de los ajustes (restaura todo `src/routes.js`, incluido `/sisap/empleado`):

```bash
git checkout -- src/routes.js
```

Después del commit, solo este endpoint: reemplazar la ruta actual por el bloque de abajo. Depende de `IS_ONLY_LN`, `diffYear` y `specificQuery`, que siguen existiendo en `routes.js`. Las constantes `SECRETARIA_DIRECCIONES_APARTE` y `NOMINA_EXCLUIDOS_CONDITION` ya no son necesarias para este bloque.

## Resumen de cómo funcionaba

- **Actividad:** `f.condicion_actividad_ficha=1`.
- **Nómina:** filtro propio por dependencia (`if` por `cod_dep`), y para `XX-YY` (dep=1) lista fija de tipos excluidos que incluye 34, 35, 36 y 37.
- **Secretaría `XX-00`:** excluye direcciones aparte: 01 → (2,3,4,5), 10 → (8,9), 13 → (2,3,4,5,8), 15 → (1,2,3,4,5,8,9).
- **Clasificación:** `hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`. Sin filtro en 1008, 1021, 1039, 1045 y 1046. Sin excluir la 12 en 1006, 1022 y 1040.
- **BD:** 1047 iba a db 1, no estaba en la lista de db 2.
- **404:** para `cod_dep` < 1000, 1024 y 1025.
- **Rama `IS_ONLY_LN`:** presente en cada condición (hoy `IS_ONLY_LN = false`).

## Código original

```js
router.get("/hoja_vida/consulta_dep/:cod_dep", async (req, res) => {
  const { cod_dep } = req.params;

  if (!cod_dep) {
    res.status(404).send("Dependencia requerida");
    return false;
  }

  const getCondition = () => {
    if (cod_dep.includes("-")) {
      const codSplit = cod_dep.split("-");
      if (codSplit.length < 2) {
        return null;
      }
      if (codSplit[1] != "00") {
        return IS_ONLY_LN
          ? `f.cod_dep=1 and f.cod_secretaria=${codSplit[0]} and f.cod_direccion=${codSplit[1]} and f.cod_tipo_nomina in (1,2,3) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`
          : `f.cod_dep=1 and f.cod_secretaria=${codSplit[0]} and f.cod_direccion=${codSplit[1]} and f.cod_tipo_nomina not in (10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,33,34,35,36,37,38,39,40,9,6) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`;
      }
      if (codSplit[0] == "01") {
        return IS_ONLY_LN
          ? `f.cod_dep=1 and f.cod_secretaria=${codSplit[0]} and f.cod_direccion NOT IN (2,3,4,5) and f.cod_tipo_nomina in (1,2,3) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`
          : `f.cod_dep=1 and f.cod_secretaria=${codSplit[0]} and f.cod_direccion NOT IN (2,3,4,5) and f.cod_tipo_nomina not in (10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,33,34,35,36,37,38,39,40,9,6) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`;
      }
      if (codSplit[0] == "10") {
        return IS_ONLY_LN
          ? `f.cod_dep=1 and f.cod_secretaria=${codSplit[0]} and f.cod_direccion NOT IN (8,9) and f.cod_tipo_nomina in (1,2,3) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`
          : `f.cod_dep=1 and f.cod_secretaria=${codSplit[0]} and f.cod_direccion NOT IN (8,9) and f.cod_tipo_nomina not in (10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,33,34,35,36,37,38,39,40,9,6) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`;
      }
      if (codSplit[0] == "13") {
        return IS_ONLY_LN
          ? `f.cod_dep=1 and f.cod_secretaria=${codSplit[0]} and f.cod_direccion NOT IN (2,3,4,5,8) and f.cod_tipo_nomina in (1,2,3) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`
          : `f.cod_dep=1 and f.cod_secretaria=${codSplit[0]} and f.cod_direccion NOT IN (2,3,4,5,8) and f.cod_tipo_nomina not in (10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,33,34,35,36,37,38,39,40,9,6) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`;
      }
      if (codSplit[0] == "15") {
        return IS_ONLY_LN
          ? `f.cod_dep=1 and f.cod_secretaria=${codSplit[0]} and f.cod_direccion NOT IN (1,2,3,4,5,8,9) and f.cod_tipo_nomina in (1,2,3) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`
          : `f.cod_dep=1 and f.cod_secretaria=${codSplit[0]} and f.cod_direccion NOT IN (1,2,3,4,5,8,9) and f.cod_tipo_nomina not in (10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,33,34,35,36,37,38,39,40,9,6) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`;
      }

      return IS_ONLY_LN
        ? `f.cod_dep=1 and f.cod_secretaria=${codSplit[0]} and f.cod_tipo_nomina in (1,2,3,29) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`
        : `f.cod_dep=1 and f.cod_secretaria=${codSplit[0]} and f.cod_tipo_nomina not in (10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,33,34,35,36,37,38,39,40,9,6) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`;
    }
    if (cod_dep < 1000 || cod_dep == 1024 || cod_dep == 1025) {
      return null;
    }
    if (cod_dep == 1000) {
      return IS_ONLY_LN
        ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1,2) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`
        : `f.cod_dep=${cod_dep} and f.cod_tipo_nomina not in (10,11) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`;
    }
    if (cod_dep == 1003) {
      return IS_ONLY_LN
        ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1,7) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`
        : `f.cod_dep=${cod_dep} and f.cod_tipo_nomina not in (7,8,9,10,11,12,13,14,15,16,17,18,19) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`;
    }
    if (cod_dep == 1004) {
      return IS_ONLY_LN
        ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1,2) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`
        : `f.cod_dep=${cod_dep} and f.cod_tipo_nomina not in (7,8,9) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`;
    }
    if (cod_dep == 1005) {
      return IS_ONLY_LN
        ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1,2) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`
        : `f.cod_dep=${cod_dep} and f.cod_tipo_nomina not in (8,9,10) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`;
    }
    if (cod_dep == 1006) {
      return IS_ONLY_LN
        ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1,2) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`
        : `f.cod_dep=${cod_dep} and f.cod_tipo_nomina not in (7,8,9,10,11,12) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,13,14)`;
    }
    if (cod_dep == 1007) {
      return IS_ONLY_LN
        ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1,2) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`
        : `f.cod_dep=${cod_dep} and f.cod_tipo_nomina not in (7) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`;
    }
    if (cod_dep == 1008) {
      return IS_ONLY_LN
        ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1,2) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`
        : `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1,2,4,5,7,10)`;
    }
    if (cod_dep == 1009) {
      return IS_ONLY_LN
        ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1,7) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`
        : `f.cod_dep=${cod_dep} and f.cod_tipo_nomina not in (10,11) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`;
    }
    if (cod_dep == 1010) {
      return IS_ONLY_LN
        ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1,2) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`
        : `f.cod_dep=${cod_dep} and f.cod_tipo_nomina not in (6,9,10,11) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`;
    }
    if (cod_dep == 1011) {
      return IS_ONLY_LN
        ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1,2) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`
        : `f.cod_dep=${cod_dep} and f.cod_tipo_nomina not in (7,8) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`;
    }
    if (cod_dep == 1012) {
      return IS_ONLY_LN
        ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1,2) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`
        : `f.cod_dep=${cod_dep} and f.cod_tipo_nomina not in (6,7) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`;
    }
    if (cod_dep == 1014) {
      return IS_ONLY_LN
        ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1,2,3,8) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`
        : `f.cod_dep=${cod_dep} and f.cod_tipo_nomina not in (3,11,12,13,14) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`;
    }
    if (cod_dep == 1015) {
      return IS_ONLY_LN
        ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1,2,3) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`
        : `f.cod_dep=${cod_dep} and f.cod_tipo_nomina not in (10,11,12,13) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`;
    }
    if (cod_dep == 1016) {
      return IS_ONLY_LN
        ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1,2) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`
        : `f.cod_dep=${cod_dep} and f.cod_tipo_nomina not in (9,10,11,12) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`;
    }
    if (cod_dep == 1021) {
      return IS_ONLY_LN
        ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1,2) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`
        : `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (2,4,5,6,7)`;
    }
    if (cod_dep == 1022) {
      return IS_ONLY_LN
        ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1,2) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`
        : `f.cod_dep=${cod_dep} and f.cod_tipo_nomina not in (9) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,13,14)`;
    }
    if (cod_dep == 1023) {
      return IS_ONLY_LN
        ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1,2) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`
        : `f.cod_dep=${cod_dep} and f.cod_tipo_nomina not in (9,10,11) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`;
    }
    if (cod_dep == 1027) {
      return IS_ONLY_LN
        ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1,2) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`
        : `f.cod_dep=${cod_dep} and f.cod_tipo_nomina not in (7,8) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`;
    }
    if (cod_dep == 1028) {
      return IS_ONLY_LN
        ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1,2,5) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`
        : `f.cod_dep=${cod_dep} and f.cod_tipo_nomina not in (6,7,8,9,10) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`;
    }
    if (cod_dep == 1035) {
      return IS_ONLY_LN
        ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1,2) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`
        : `f.cod_dep=${cod_dep} and f.cod_tipo_nomina not in (6,7,8,9) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`;
    }
    if (cod_dep == 1037) {
      return IS_ONLY_LN
        ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1,2) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`
        : `f.cod_dep=${cod_dep} and f.cod_tipo_nomina not in (8,9) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`;
    }
    if (cod_dep == 1039) {
      return IS_ONLY_LN ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)` : `f.cod_dep=${cod_dep}`;
    }
    if (cod_dep == 1040) {
      return IS_ONLY_LN
        ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1, 2, 3) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`
        : `f.cod_dep=${cod_dep} and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,13,14)`;
    }
    if (cod_dep == 1041) {
      return IS_ONLY_LN
        ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1, 2) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`
        : `f.cod_dep=${cod_dep} and f.cod_tipo_nomina not in (6,7,8,9) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`;
    }
    if (cod_dep == 1043) {
      return IS_ONLY_LN
        ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1,2) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`
        : `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1,2,4) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`;
    }
    if (cod_dep == 1045) {
      return IS_ONLY_LN
        ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1,2) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`
        : `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1,2,4)`;
    }
    if (cod_dep == 1046) {
      return IS_ONLY_LN ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1,2) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)` : `f.cod_dep=${cod_dep}`;
    }
    return IS_ONLY_LN
      ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1,2) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`
      : `f.cod_dep=${cod_dep} and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`;
  };

  const getConditionDB = () => {
    if (cod_dep.includes("-")) {
      return 1;
    }
    if (cod_dep < 1000) {
      return 1;
    }
    if (
      cod_dep == 1006 ||
      cod_dep == 1021 ||
      cod_dep == 1027 ||
      cod_dep == 1028 ||
      cod_dep == 1036 ||
      cod_dep == 1037 ||
      cod_dep == 1038 ||
      cod_dep == 1039 ||
      cod_dep == 1040 ||
      cod_dep == 1042 ||
      cod_dep == 1043 ||
      cod_dep == 1045 ||
      cod_dep == 1046
    ) {
      return 2;
    }
    if (cod_dep == 1035) {
      return 3;
    }
    if (cod_dep == 1041) {
      return 4;
    }
    return 1;
  };

  const condition_dep = getCondition();
  if (condition_dep === null) {
    res.status(404).send("Dependencia requerida");
    return false;
  }

  if (condition_dep === "") {
    res.status(404).send({ message: "No existe resultados", error: "" });
    return false;
  }

  try {
    let db = getConditionDB();
    let beneficiario = [];
    const CURRENT_YEAR = new Date().getFullYear();
    const sqlQuery = `SELECT f.cedula_identidad, f.primer_nombre || ' ' || f.segundo_nombre || ' ' || f.primer_apellido || ' ' || f.segundo_apellido as nombre, 
    f.deno_cod_secretaria, f.cod_secretaria, f.deno_cod_direccion, f.cod_direccion, f.deno_cod_division, f.cod_division, f.deno_cod_departamento, f.cod_departamento, f.demonimacion_puesto, f.cod_dep, f.denominacion_dependencia, f.cod_ficha, f.fecha_nacimiento, f.sexo, CASE 
            WHEN f.estado_civil='S' THEN 'Soltero' 
            WHEN f.estado_civil='C' THEN 'Casado' 
            WHEN f.estado_civil='D' THEN 'Divorciado' 
            ELSE 'Otro'
          END estado_civil, f.grupo_sanguineo, 
    f.correo_electronico,
    f.deno_cod_estado, f.deno_cod_municipio, f.deno_cod_parroquia, f.deno_cod_centro, f.deno_ciudad,    
    (select denominacion from cugd01_estados where cod_republica=f.cod_pais_origen and cod_estado=f.cod_estado_origen) as deno_estado_nacimiento,    
    (select denominacion from cugd01_municipios where cod_republica=f.cod_pais_origen and cod_estado=f.cod_estado_origen and cod_municipio=f.cod_municipio_origen) as deno_municipio_nacimiento,    
    (select denominacion from cugd01_parroquias where cod_republica=f.cod_pais_origen and cod_estado=f.cod_estado_origen and cod_municipio=f.cod_municipio_origen and cod_parroquia=f.cod_parroquia_origen) as deno_parroquia_nacimiento,
    (select denominacion from cugd01_centros_poblados where cod_republica=f.cod_pais_origen and cod_estado=f.cod_estado_origen and cod_municipio=f.cod_municipio_origen and cod_parroquia=f.cod_parroquia_origen and cod_centro=f.cod_centropoblado_origen) as deno_centropoblado_nacimiento,
    (select denominacion from cugd01_estados where cod_republica=1 and cod_estado=f.cod_estado_habitacion) as deno_estado_habitacion,    
    (select denominacion from cugd01_municipios where cod_republica=1 and cod_estado=f.cod_estado_habitacion and cod_municipio=f.cod_municipio_habitacion) as deno_municipio_habitacion,    
    (select denominacion from cugd01_parroquias where cod_republica=1 and cod_estado=f.cod_estado_habitacion and cod_municipio=f.cod_municipio_habitacion and cod_parroquia=f.cod_parroquia_habitacion) as deno_parroquia_habitacion,    
    (select denominacion from cugd01_centros_poblados where cod_republica=1 and cod_estado=f.cod_estado_habitacion and cod_municipio=f.cod_municipio_habitacion and cod_parroquia=f.cod_parroquia_habitacion and cod_centro=f.cod_centropoblado_habitacion) as deno_contropoblado_habitacion,
    f.cod_centropoblado_habitacion,
    (select denominacion from cnmd06_profesiones where cod_profesion=dp.cod_profesion) as profesion,
    (select denominacion from cnmd06_especialidades where cod_profesion=dp.cod_profesion and cod_especialidad=dp.cod_especialidad) as especialidad,
    f.fecha_ingreso, f.direccion_habitacion, f.telefonos_habitacion, f.carnet,
    (select devolver_grado_puesto(
      (select xy.clasificacion_personal from cnmd01 xy where xy.cod_dep=t.cod_dep and xy.cod_tipo_nomina=t.cod_tipo_nomina), t.cod_puesto) )as cod_grado_puesto,
      hn.cod_tipo_nomina, hn.denominacion, hn.denominacion_devengado
    FROM v_cnmd06_fichas_2 as f 
    FULL OUTER JOIN cnmd05 as t on f.cod_dep=t.cod_dep and f.cod_ficha=t.cod_ficha and f.cod_cargo=t.cod_cargo and t.cod_tipo_nomina=f.cod_tipo_nomina
    FULL OUTER JOIN cnmd01 as hn on hn.cod_dep=t.cod_dep and hn.cod_tipo_nomina=t.cod_tipo_nomina 
    FULL OUTER JOIN cnmd06_datos_personales as dp on dp.cedula_identidad=f.cedula_identidad 
    where  ${condition_dep} and t.ano=${CURRENT_YEAR} and f.condicion_actividad_ficha=1 and hn.denominacion_devengado<>'PRESTACIONES' [condition_ext] ORDER BY t.cod_tipo_nomina`;
    /*
f.cedula_identidad not in(17272487, 3121415,3516898,3953350,4285183,4877107,5008142,5621980,5622394,6345849,6405730,6626232,6626277,6627482,6641554,6940025,7278034,7279615,7292652,7295995,7947144,8173284,8413995,8552594,8570155,8574193,8620863,8621316,8627134,8783227,8783879,8784457,8784657,8786434,8789992,8791843,8798328,8799042,8803322,8803363,8909143,8996101,8999871,9591223,9596950,9875740,9883031,9883515,9886234,9889227,10266922,10269651,10273815,10665103,10667050,10670434,10980752,11053128,11115431,11120282,11237678,11367254,11369489,11757562,11759500,11795566,11796841,11844546,12119137,12322982,12323559,12410854,12512086,12897477,13152298,13153437,13237082,13794978,13860220,14344452,14520381,14538971,14539214,14693507,14871624,14948026,15152257,15248588,15301671,15452027,15452574,15480321,15681918,15710817,16383483,16506971,16511064,16790345,16913325,17272513,17353679,17746355,17936870,18328167,18543178,18543198,18894872,19019256,19067800,19100721,19246030,19405492,19473610,19657990,19724913,19959783,19986482,20184489,20247847,20588884,20876101,21177638,21581339,21658471,23647339,24792900,25419847,27238440,30811163,3167720,6800718,20528474,8630884,11236283,10499354,7291076,16144933,15248175,8790968,10983518,10497643,14294391,12512650,12899227,12475710,9918931,14672586,16506165,17001528,13680322,9874646,8791481,6997740,12117664,8630784,22614807,16383567,22882050,12766040,16504587,11369286,16236363,8558331,18067410,8805348,5072098,11842698,15100091,19759639,10496825,17374212,11368831,18376350,12582770,13482691,14191166,15811760,4345626,8728909,19725594,24678325,82050877,9883003,4545380,27631216,8151814,8556221,10855142,13874018,16075260,16803261,17166165,19222422,19724480,19948836,24976488,2517455,3581077,5153837,5157088,6450664,9889039,9889428,12840505,12841010,2516732,4388634,5430469,6002161,7281697,7294482,8995885,8996437,11116812,14146614,10981600,13571432,20878323,9886484,9888274,11124321,12842961,13622436,14870473,15710663,21147972,3951934,3958684,4284675,4391053,4798843,5157359,7275674,7276672,8554666,8784662,9886011,9888135,10668280,10670925,10671144,11090574,14645308,14870019,16912812,17513856,26081962,27238537,5523033,19725353) and
 */
    const query = await specificQuery({ sqlQuery, table: "f.", db: db });
    if (query.length > 0) {
      query.map((emp) => {
        beneficiario.push({
          ...emp,
          edad: diffYear(emp.fecha_nacimiento),
          antiguedad: diffYear(emp.fecha_ingreso),
        });
      });
    }
    /*const checkQuery = Object.values(query).reduce((acc, current) => acc + current.length, 0);

    if (checkQuery > 0) {
      if (query.result_db1.length > 0) {
        query.result_db1.map((emp) => {
          beneficiario.push({
            ...emp,
            edad: diffYear(emp.fecha_nacimiento),
            antiguedad: diffYear(emp.fecha_ingreso),
          });
        });

        if (beneficiario.length > 0) {
          db = 1;
        }
      } else if (query.result_db2.length > 0) {
        query.result_db2.map((emp) => {
          beneficiario.push({
            ...emp,
            edad: diffYear(emp.fecha_nacimiento),
            antiguedad: diffYear(emp.fecha_ingreso),
          });
        });

        if (beneficiario.length > 0) {
          db = 2;
        }
      } else if (query.result_db3.length > 0) {
        query.result_db3.map((emp) => {
          beneficiario.push({
            ...emp,
            edad: diffYear(emp.fecha_nacimiento),
            antiguedad: diffYear(emp.fecha_ingreso),
          });
        });

        if (beneficiario.length > 0) {
          db = 3;
        }
      } else if (query.result_db4.length > 0) {
        query.result_db4.map((emp) => {
          beneficiario.push({
            ...emp,
            edad: diffYear(emp.fecha_nacimiento),
            antiguedad: diffYear(emp.fecha_ingreso),
          });
        });

        if (beneficiario.length > 0) {
          db = 4;
        }
      }
    }*/

    if (beneficiario.length > 0) {
      const result_employee = beneficiario; //query;
      res.json(result_employee);
      return true;
    } else {
      res.status(404).send({ message: "No existe resultados", error: "" });
      return false;
    }
  } catch (error) {
    res.status(500).send({ message: "Error en la consulta unificada", error: error.message });
  }
});
```
