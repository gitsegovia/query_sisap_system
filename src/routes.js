import express from "express";
import unifiedQuery, { identifiedQuery, specificQuery } from "./sequelizedb";

const IS_ONLY_LN = false;

function diffYear(date) {
  const current = new Date();
  const formatted_date = new Date(date);
  let year = current.getFullYear() - formatted_date.getFullYear();
  const month = current.getMonth() - formatted_date.getMonth();

  if (month < 0 || (month === 0 && current.getDate() < formatted_date.getDate())) {
    year--;
  }

  return year;
}

const router = express.Router();
//Assets
//router.use('/assets', express.static(path.join(__dirname, '../public/assets')));

//Routes
router.get("/fusamiebg/consulta/:cedula?", async (req, res) => {
  const { cedula } = req.params;

  if (!cedula) {
    res.status(404).send("Cedula requerida");
    return false;
  }

  try {
    const VALID_DAYS = 30;
    let valid = false;
    let db = 1;
    let periodo_desde = "";
    let beneficiario = "";
    const CURRENT_YEAR = new Date().getFullYear();
    const sqlQuery = `SELECT cedula_identidad, primer_nombre || ' ' || segundo_nombre || ' ' || primer_apellido || ' ' || segundo_apellido as nombre, f.deno_cod_secretaria, f.deno_cod_direccion, f.demonimacion_puesto, f.cod_dep, f.cod_ficha, f.fecha_nacimiento, f.sexo, f.grupo_sanguineo, f.fecha_ingreso, f.direccion_habitacion, f.telefonos_habitacion, f.carnet, hn.periodo_desde FROM v_cnmd06_fichas_2 as f FULL OUTER JOIN cnmd08_historia_transacciones as t on f.cod_ficha=t.cod_ficha and f.cod_cargo=t.cod_cargo FULL OUTER JOIN cnmd08_historia_nomina as hn on hn.cod_dep=t.cod_dep and hn.cod_tipo_nomina=t.cod_tipo_nomina and hn.numero_nomina=t.numero_nomina and hn.ano=t.ano where f.cedula_identidad=${cedula} and t.ano=${CURRENT_YEAR} and t.cod_transaccion=103 [condition_ext] order by hn.periodo_desde DESC LIMIT 1`;

    const query = await specificQuery({ sqlQuery, table: "t.", db: 1 });
    const checkQuery = query.length; // Object.values(query).reduce((acc, current) => acc + current.length, 0);

    if (checkQuery > 0) {
      const current = new Date();

      if (query.length > 0) {
        const date = new Date(query[0].periodo_desde);
        const diffInTime = current.getTime() - date.getTime();
        const diffInDays = Math.ceil(diffInTime / (1000 * 3600 * 24));
        valid = diffInDays <= VALID_DAYS;
        beneficiario = query[0];
        periodo_desde = date;
      }
      // a todos los sisap
      /*
      if (query.result_db1.length > 0) {
        const date = new Date(query.result_db1[0].periodo_desde);
        const diffInTime = current.getTime() - date.getTime();
        const diffInDays = Math.ceil(diffInTime / (1000 * 3600 * 24));
        valid = diffInDays <= VALID_DAYS;
        beneficiario = query.result_db1[0];
        periodo_desde = date;
      } else if (query.result_db2.length > 0) {
        const date = new Date(query.result_db2[0].periodo_desde);
        const diffInTime = current.getTime() - date.getTime();
        const diffInDays = Math.ceil(diffInTime / (1000 * 3600 * 24));

        valid = diffInDays <= VALID_DAYS;
        beneficiario = query.result_db2[0];
        periodo_desde = date;
        db = 2;
      } else if (query.result_db3.length > 0) {
        const date = new Date(query.result_db3[0].periodo_desde);
        const diffInTime = current.getTime() - date.getTime();
        const diffInDays = Math.ceil(diffInTime / (1000 * 3600 * 24));

        valid = diffInDays <= VALID_DAYS;
        beneficiario = query.result_db3[0];
        periodo_desde = date;
        db = 3;
      } else if (query.result_db4.length > 0) {
        const date = new Date(query.result_db4[0].periodo_desde);
        const diffInTime = current.getTime() - date.getTime();
        const diffInDays = Math.ceil(diffInTime / (1000 * 3600 * 24));

        valid = diffInDays <= VALID_DAYS;
        beneficiario = query.result_db4[0];
        periodo_desde = date;
        db = 4;
      }
      */
    } else {
      const sqlQuery_obrero = `SELECT cedula_identidad, primer_nombre || ' ' || segundo_nombre || ' ' || primer_apellido || ' ' || segundo_apellido as nombre, 
      f.deno_cod_secretaria, f.deno_cod_direccion, f.demonimacion_puesto, f.cod_dep, f.cod_ficha, f.fecha_nacimiento, f.sexo, f.grupo_sanguineo, 
      f.fecha_ingreso, f.direccion_habitacion, f.telefonos_habitacion, f.carnet, hn.periodo_desde FROM v_cnmd06_fichas_2 as f FULL OUTER JOIN cnmd05 as t on f.cod_ficha=t.cod_ficha and f.cod_cargo=t.cod_cargo FULL OUTER JOIN cnmd01 as hn on hn.cod_dep=t.cod_dep and hn.cod_tipo_nomina=t.cod_tipo_nomina where f.cedula_identidad=${cedula} and t.ano=${CURRENT_YEAR} and f.condicion_actividad_ficha=1 and hn.clasificacion_personal in (2,8,10) [condition_ext] order by hn.periodo_desde DESC LIMIT 1`;
      const query_obrero = await specificQuery({ sqlQuery: sqlQuery_obrero, table: "t.", db: 1 });

      const checkQuery = query_obrero.length; //Object.values(query_obrero).reduce((acc, current) => acc + current.length, 0);

      if (checkQuery > 0) {
        valid = true;
        const current = new Date();
        //periodo_desde = new Date();
        if (query_obrero.length > 0) {
          const date = new Date(query_obrero[0].periodo_desde);
          const diffInTime = current.getTime() - date.getTime();
          const diffInDays = Math.ceil(diffInTime / (1000 * 3600 * 24));
          valid = diffInDays <= VALID_DAYS;
          beneficiario = query_obrero[0];
          periodo_desde = date;
        }
        // a todos los sisap
        /*
        if (query_obrero.result_db1.length > 0) {
          beneficiario = query_obrero.result_db1[0];
          db = 1;
        } else if (query_obrero.result_db2.length > 0) {
          beneficiario = query_obrero.result_db2[0];
          db = 2;
        } else if (query_obrero.result_db3.length > 0) {
          beneficiario = query_obrero.result_db3[0];
          db = 3;
        } else if (query_obrero.result_db4.length > 0) {
          beneficiario = query_obrero.result_db4[0];
          db = 4;
        }
        */
      }
    }

    if (valid) {
      const sqlQuery_hijos = `SELECT * FROM cnmd06_datos_hijos WHERE cedula=${cedula}`;
      const query_hijos = await specificQuery({ sqlQuery: sqlQuery_hijos, db });

      const sqlQuery_familiares = `SELECT * FROM cnmd06_datos_familiares WHERE cedula=${cedula} and cod_parentesco in (3,4,5,6,7,8)`;
      const query_familiares = await specificQuery({ sqlQuery: sqlQuery_familiares, db });

      const result_employee = {
        beneficiario: beneficiario,
        afiliacion: true,
        hijos: [...query_hijos],
        datos_familiares: [...query_familiares],
        fecha_ultimo_pago: periodo_desde,
      };
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

router.get("/carnet/consulta/:cedula?", async (req, res) => {
  const { cedula } = req.params;

  if (!cedula) {
    res.status(404).send("Cedula requerida");
    return false;
  }

  try {
    let valid = false;
    let db = 1;
    let beneficiario = "";
    const CURRENT_YEAR = new Date().getFullYear();
    const sqlQuery = `SELECT cedula_identidad, primer_nombre || ' ' || segundo_nombre || ' ' || primer_apellido || ' ' || segundo_apellido as nombre, 
    f.deno_cod_secretaria, f.deno_cod_direccion, f.demonimacion_puesto, f.cod_dep, f.cod_ficha, f.fecha_nacimiento, f.sexo, f.grupo_sanguineo, 
    f.fecha_ingreso, f.direccion_habitacion, f.telefonos_habitacion, f.carnet FROM v_cnmd06_fichas_2 as f FULL OUTER JOIN cnmd05 as t on f.cod_ficha=t.cod_ficha and f.cod_cargo=t.cod_cargo FULL OUTER JOIN cnmd01 as hn on hn.cod_dep=t.cod_dep and hn.cod_tipo_nomina=t.cod_tipo_nomina where f.cedula_identidad=${cedula} and t.ano=${CURRENT_YEAR} and f.condicion_actividad_ficha=1 [condition_ext]`;

    const query = await identifiedQuery({ sqlQuery, table: "f." });
    const checkQuery = Object.values(query).reduce((acc, current) => acc + current.length, 0);

    if (checkQuery > 0) {
      valid = true;
      if (query.result_db1.length > 0) {
        beneficiario = query.result_db1[0];
        db = 1;
      } else if (query.result_db2.length > 0) {
        beneficiario = query.result_db2[0];
        db = 2;
      } else if (query.result_db3.length > 0) {
        beneficiario = query.result_db3[0];
        db = 3;
      } else if (query.result_db4.length > 0) {
        beneficiario = query.result_db4[0];
        db = 4;
      }
    }

    if (valid) {
      const result_employee = {
        ...beneficiario,
        edad: diffYear(beneficiario.fecha_nacimiento),
        antiguedad: diffYear(beneficiario.fecha_ingreso),
      };
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

router.get("/hoja_vida/consulta/:cedula", async (req, res) => {
  const { cedula } = req.params;

  if (!cedula) {
    res.status(404).send("Cedula requerida");
    return false;
  }

  try {
    let valid = false;
    let db = 1;
    let beneficiario = "";
    const CURRENT_YEAR = new Date().getFullYear();
    const where = IS_ONLY_LN
      ? `f.cedula_identidad=${cedula} and t.ano=${CURRENT_YEAR} and f.condicion_actividad_ficha=1 and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14) [condition_ext]`
      : `f.cedula_identidad = ${cedula} and t.ano = ${CURRENT_YEAR} and f.condicion_actividad_ficha = 1 [condition_ext]`;

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
    (select denominacion from cnmd06_profesiones where cod_profesion=dp.cod_profesion) as profesion,
    (select denominacion from cnmd06_especialidades where cod_profesion=dp.cod_profesion and cod_especialidad=dp.cod_especialidad) as especialidad,
    f.fecha_ingreso, f.direccion_habitacion, f.telefonos_habitacion, f.carnet,
    (select devolver_grado_puesto(
      (select xy.clasificacion_personal from cnmd01 xy where xy.cod_dep=t.cod_dep and xy.cod_tipo_nomina=t.cod_tipo_nomina), t.cod_puesto) )as cod_grado_puesto, hn.denominacion
    FROM v_cnmd06_fichas_2 as f 
    FULL OUTER JOIN cnmd05 as t on f.cod_dep=t.cod_dep and f.cod_ficha=t.cod_ficha and f.cod_cargo=t.cod_cargo 
    FULL OUTER JOIN cnmd01 as hn on hn.cod_dep=t.cod_dep and hn.cod_tipo_nomina=t.cod_tipo_nomina 
    FULL OUTER JOIN cnmd06_datos_personales as dp on dp.cedula_identidad=f.cedula_identidad 
    where ${where}`;

    const query = await identifiedQuery({ sqlQuery, table: "f." });
    const checkQuery = Object.values(query).reduce((acc, current) => acc + current.length, 0);

    /* con validadcion
    if (checkQuery > 0) {
      if (query.result_db1.length > 0) {
        if (query.result_db1[0].cod_grado_puesto == 99) {
          valid = true;
          beneficiario = query.result_db1[0];
          db = 1;
        }
      } else if (query.result_db2.length > 0) {
        if (query.result_db2[0].cod_grado_puesto == 99) {
          valid = true;
          beneficiario = query.result_db2[0];
          db = 2;
        }
      } else if (query.result_db3.length > 0) {
        if (query.result_db3[0].cod_grado_puesto == 99) {
          valid = true;
          beneficiario = query.result_db3[0];
          db = 3;
        }
      } else if (query.result_db4.length > 0) {
        if (query.result_db4[0].cod_grado_puesto == 99) {
          valid = true;
          beneficiario = query.result_db4[0];
          db = 4;
        }
      }
    }*/
    if (checkQuery > 0) {
      if (query.result_db1.length > 0) {
        valid = true;
        beneficiario = query.result_db1[0];
        db = 1;
      } else if (query.result_db2.length > 0) {
        valid = true;
        beneficiario = query.result_db2[0];
        db = 2;
      } else if (query.result_db3.length > 0) {
        valid = true;
        beneficiario = query.result_db3[0];
        db = 3;
      } else if (query.result_db4.length > 0) {
        valid = true;
        beneficiario = query.result_db4[0];
        db = 4;
      }
    }

    if (valid) {
      const sqlQueryFp = `SELECT deno_curso, deno_institucion, duracion, desde, hasta, observaciones
    FROM v_cnmd06_datos_formacion_profesional  
    where cedula=${cedula}`;

      const queryFp = await specificQuery({ sqlQuery: sqlQueryFp, db });
      const result_employee = {
        ...beneficiario,
        formacion_profesional: queryFp,
        edad: diffYear(beneficiario.fecha_nacimiento),
        antiguedad: diffYear(beneficiario.fecha_ingreso),
      };
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

router.get("/hoja_vida/consulta_foto/:cedula", async (req, res) => {
  const { cedula } = req.params;

  if (!cedula) {
    res.status(404).send("Cedula requerida");
    return false;
  }

  try {
    let valid = false;
    let db = 1;
    let beneficiario = "";
    const CURRENT_YEAR = new Date().getFullYear();

    const where = IS_ONLY_LN
      ? `f.cedula_identidad=${cedula} and t.ano=${CURRENT_YEAR} and f.condicion_actividad_ficha=1 and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14) [condition_ext]`
      : `f.cedula_identidad=${cedula} and t.ano=${CURRENT_YEAR} and f.condicion_actividad_ficha=1 and hn.clasificacion_personal in (1,17,18) [condition_ext]`;

    const sqlQuery = `SELECT f.cedula_identidad
    FROM v_cnmd06_fichas_2 as f 
    FULL OUTER JOIN cnmd05 as t on f.cod_dep=t.cod_dep and f.cod_ficha=t.cod_ficha and f.cod_cargo=t.cod_cargo 
    FULL OUTER JOIN cnmd01 as hn on hn.cod_dep=t.cod_dep and hn.cod_tipo_nomina=t.cod_tipo_nomina 
    FULL OUTER JOIN cnmd06_datos_personales as dp on dp.cedula_identidad=f.cedula_identidad 
    where ${where}`;

    const query = await identifiedQuery({ sqlQuery, table: "f." });
    const checkQuery = Object.values(query).reduce((acc, current) => acc + current.length, 0);

    /* con validadcion
    if (checkQuery > 0) {
      if (query.result_db1.length > 0) {
        if (query.result_db1[0].cod_grado_puesto == 99) {
          valid = true;
          beneficiario = query.result_db1[0];
          db = 1;
        }
      } else if (query.result_db2.length > 0) {
        if (query.result_db2[0].cod_grado_puesto == 99) {
          valid = true;
          beneficiario = query.result_db2[0];
          db = 2;
        }
      } else if (query.result_db3.length > 0) {
        if (query.result_db3[0].cod_grado_puesto == 99) {
          valid = true;
          beneficiario = query.result_db3[0];
          db = 3;
        }
      } else if (query.result_db4.length > 0) {
        if (query.result_db4[0].cod_grado_puesto == 99) {
          valid = true;
          beneficiario = query.result_db4[0];
          db = 4;
        }
      }
    }*/
    if (checkQuery > 0) {
      if (query.result_db1.length > 0) {
        valid = true;
        beneficiario = query.result_db1[0];
        db = 1;
      } else if (query.result_db2.length > 0) {
        valid = true;
        beneficiario = query.result_db2[0];
        db = 2;
      } else if (query.result_db3.length > 0) {
        valid = true;
        beneficiario = query.result_db3[0];
        db = 3;
      } else if (query.result_db4.length > 0) {
        valid = true;
        beneficiario = query.result_db4[0];
        db = 4;
      }
    }

    if (valid) {
      const sqlQueryFoto = `SELECT imagen, tipo FROM cugd10_imagenes  
    where cod_campo=11 and identificacion='${cedula}'`;

      const queryFoto = await specificQuery({ sqlQuery: sqlQueryFoto, db });
      console.log(queryFoto);
      if (queryFoto.length > 0) {
        const imagenBytes = queryFoto[0].imagen;
        const imagenBuffer = Buffer.from(imagenBytes, "base64");

        res.writeHead(200, {
          "Content-Type": queryFoto[0].tipo, // Ajusta el tipo de contenido según tu caso
          "Content-Length": imagenBuffer.length,
        });
        res.end(imagenBuffer);
        return true;
      } else {
        res.status(404).send("No posee imagen");
        return false;
      }
    } else {
      res.status(404).send({ message: "No existe resultados", error: "" });
      return false;
    }
  } catch (error) {
    res.status(500).send({ message: "Error en la consulta unificada", error: error.message });
  }
});

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
          ? `f.cod_dep=1 and f.cod_secretaria=${codSplit[0]} and f.cod_direccion=${codSplit[1]} and f.cod_tipo_nomina in (1,2,3)`
          : `f.cod_dep=1 and f.cod_secretaria=${codSplit[0]} and f.cod_direccion=${codSplit[1]} and f.cod_tipo_nomina not in (10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,33,37,38,39,40,9)`;
      }
      if (codSplit[0] == "01") {
        return IS_ONLY_LN
          ? `f.cod_dep=1 and f.cod_secretaria=${codSplit[0]} and f.cod_direccion NOT IN (2,3,4,5) and f.cod_tipo_nomina in (1,2,3)`
          : `f.cod_dep=1 and f.cod_secretaria=${codSplit[0]} and f.cod_direccion NOT IN (2,3,4,5) and f.cod_tipo_nomina not in (10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,33,37,38,39,40,9)`;
      }
      if (codSplit[0] == "10") {
        return IS_ONLY_LN
          ? `f.cod_dep=1 and f.cod_secretaria=${codSplit[0]} and f.cod_direccion NOT IN (8,9) and f.cod_tipo_nomina in (1,2,3)`
          : `f.cod_dep=1 and f.cod_secretaria=${codSplit[0]} and f.cod_direccion NOT IN (8,9) and f.cod_tipo_nomina not in (10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,33,37,38,39,40,9)`;
      }
      if (codSplit[0] == "13") {
        return IS_ONLY_LN
          ? `f.cod_dep=1 and f.cod_secretaria=${codSplit[0]} and f.cod_direccion NOT IN (2,3,4,5,8) and f.cod_tipo_nomina in (1,2,3)`
          : `f.cod_dep=1 and f.cod_secretaria=${codSplit[0]} and f.cod_direccion NOT IN (2,3,4,5,8) and f.cod_tipo_nomina not in (10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,33,37,38,39,40,9)`;
      }
      if (codSplit[0] == "15") {
        return IS_ONLY_LN
          ? `f.cod_dep=1 and f.cod_secretaria=${codSplit[0]} and f.cod_direccion NOT IN (1,2,3,4,5,8,9) and f.cod_tipo_nomina in (1,2,3)`
          : `f.cod_dep=1 and f.cod_secretaria=${codSplit[0]} and f.cod_direccion NOT IN (1,2,3,4,5,8,9) and f.cod_tipo_nomina not in (10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,33,37,38,39,40,9)`;
      }

      return IS_ONLY_LN
        ? `f.cod_dep=1 and f.cod_secretaria=${codSplit[0]} and f.cod_tipo_nomina in (1,2,3,29)`
        : `f.cod_dep=1 and f.cod_secretaria=${codSplit[0]} and f.cod_tipo_nomina not in (10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,33,37,38,39,40,9)`;
    }
    if (cod_dep < 1000 || cod_dep == 1024 || cod_dep == 1025) {
      return null;
    }
    if (cod_dep == 1000) {
      return IS_ONLY_LN ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1,2)` : `f.cod_dep=${cod_dep} and f.cod_tipo_nomina not in (10,11)`;
    }
    if (cod_dep == 1003) {
      return IS_ONLY_LN ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1,7)` : `f.cod_dep=${cod_dep} and f.cod_tipo_nomina not in (7,8,9,10,11,12,13,14,15,16,17,18,19)`;
    }
    if (cod_dep == 1004) {
      return IS_ONLY_LN ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1,2)` : `f.cod_dep=${cod_dep} and f.cod_tipo_nomina not in (7,8,9)`;
    }
    if (cod_dep == 1005) {
      return IS_ONLY_LN ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1,2)` : `f.cod_dep=${cod_dep} and f.cod_tipo_nomina not in (8,9,10)`;
    }
    if (cod_dep == 1006) {
      return IS_ONLY_LN ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1,2)` : `f.cod_dep=${cod_dep} and f.cod_tipo_nomina not in (7,8,9,10,11,12)`;
    }
    if (cod_dep == 1007) {
      return IS_ONLY_LN ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1,2)` : `f.cod_dep=${cod_dep} and f.cod_tipo_nomina not in (7)`;
    }
    if (cod_dep == 1008) {
      return IS_ONLY_LN ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1,2)` : `f.cod_dep=${cod_dep} and f.cod_tipo_nomina not in (11,12,13)`;
    }
    if (cod_dep == 1009) {
      return IS_ONLY_LN ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1,7)` : `f.cod_dep=${cod_dep} and f.cod_tipo_nomina not in (10,11)`;
    }
    if (cod_dep == 1010) {
      return IS_ONLY_LN ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1,2)` : `f.cod_dep=${cod_dep} and f.cod_tipo_nomina not in (9,10,11)`;
    }
    if (cod_dep == 1011) {
      return IS_ONLY_LN ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1,2)` : `f.cod_dep=${cod_dep} and f.cod_tipo_nomina not in (7,8)`;
    }
    if (cod_dep == 1012) {
      return IS_ONLY_LN ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1,2)` : `f.cod_dep=${cod_dep} and f.cod_tipo_nomina not in (6,7)`;
    }
    if (cod_dep == 1014) {
      return IS_ONLY_LN ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1,2,3,8)` : `f.cod_dep=${cod_dep} and f.cod_tipo_nomina not in (11,12,13,14)`;
    }
    if (cod_dep == 1015) {
      return IS_ONLY_LN ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1,2,3)` : `f.cod_dep=${cod_dep} and f.cod_tipo_nomina not in (10,11,12,13)`;
    }
    if (cod_dep == 1016) {
      return IS_ONLY_LN ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1,2)` : `f.cod_dep=${cod_dep} and f.cod_tipo_nomina not in (9,10,11,12)`;
    }
    if (cod_dep == 1021) {
      return IS_ONLY_LN ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1,2)` : `f.cod_dep=${cod_dep} and f.cod_tipo_nomina not in (11)`;
    }
    if (cod_dep == 1022) {
      return IS_ONLY_LN ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1,2)` : `f.cod_dep=${cod_dep} and f.cod_tipo_nomina not in (9)`;
    }
    if (cod_dep == 1023) {
      return IS_ONLY_LN ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1,2)` : `f.cod_dep=${cod_dep} and f.cod_tipo_nomina not in (9,10,11)`;
    }
    if (cod_dep == 1027) {
      return IS_ONLY_LN ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1,2)` : `f.cod_dep=${cod_dep} and f.cod_tipo_nomina not in (7,8)`;
    }
    if (cod_dep == 1028) {
      return IS_ONLY_LN ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1,2,5)` : `f.cod_dep=${cod_dep} and f.cod_tipo_nomina not in (7,8,9,10)`;
    }
    if (cod_dep == 1035) {
      return IS_ONLY_LN ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1,2)` : `f.cod_dep=${cod_dep} and f.cod_tipo_nomina not in (6,7,8,9)`;
    }
    if (cod_dep == 1037) {
      return IS_ONLY_LN ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1,2)` : `f.cod_dep=${cod_dep} and f.cod_tipo_nomina not in (8,9)`;
    }
    if (cod_dep == 1039) {
      return IS_ONLY_LN ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1)` : `f.cod_dep=${cod_dep}`;
    }
    if (cod_dep == 1040) {
      return IS_ONLY_LN ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1, 2, 3)` : `f.cod_dep=${cod_dep}`;
    }
    if (cod_dep == 1041) {
      return IS_ONLY_LN ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1, 2)` : `f.cod_dep=${cod_dep} and f.cod_tipo_nomina not in (6,7,8,9)`;
    }

    return IS_ONLY_LN ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1,2)` : `f.cod_dep=${cod_dep}`;
  };

  const getConditionDB = () => {
    if (cod_dep.includes("-")) {
      return 1;
    }
    if (cod_dep < 1000) {
      return 1;
    }
    if (cod_dep == 1006 || cod_dep == 1027 || cod_dep == 1028 || cod_dep == 1036 || cod_dep == 1037 || cod_dep == 1038 || cod_dep == 1039 || cod_dep == 1045) {
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
    (select denominacion from cnmd06_profesiones where cod_profesion=dp.cod_profesion) as profesion,
    (select denominacion from cnmd06_especialidades where cod_profesion=dp.cod_profesion and cod_especialidad=dp.cod_especialidad) as especialidad,
    f.fecha_ingreso, f.direccion_habitacion, f.telefonos_habitacion, f.carnet,
    (select devolver_grado_puesto(
      (select xy.clasificacion_personal from cnmd01 xy where xy.cod_dep=t.cod_dep and xy.cod_tipo_nomina=t.cod_tipo_nomina), t.cod_puesto) )as cod_grado_puesto,
      hn.cod_tipo_nomina, han.denominacion
    FROM v_cnmd06_fichas_2 as f 
    FULL OUTER JOIN cnmd05 as t on f.cod_dep=t.cod_dep and f.cod_ficha=t.cod_ficha and f.cod_cargo=t.cod_cargo and t.cod_tipo_nomina=f.cod_tipo_nomina
    FULL OUTER JOIN cnmd01 as hn on hn.cod_dep=t.cod_dep and hn.cod_tipo_nomina=t.cod_tipo_nomina 
    FULL OUTER JOIN cnmd06_datos_personales as dp on dp.cedula_identidad=f.cedula_identidad 
    where ${condition_dep} and t.ano=${CURRENT_YEAR} and f.condicion_actividad_ficha=1 and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14) [condition_ext] ORDER BY t.cod_tipo_nomina`;

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

router.get("/hoja_vida/lista_empleados/", async (req, res) => {
  try {
    let condition = "";
    const dep = ["01-3", "01-4", "01-5", "10-8", "10-9", "13-2", "13-3", "13-4", "13-5", "13-8", "15-1", "15-2", "15-4", "15-8", "15-9"];

    condition = IS_ONLY_LN
      ? condition.concat(`( f.cod_dep=1 and f.cod_secretaria=1 and f.cod_direccion NOT IN (2,3,4,5) and f.cod_tipo_nomina in (1,2,3) ) `)
      : condition.concat(
          `( f.cod_dep=1 and f.cod_secretaria=1 and f.cod_direccion NOT IN (2,3,4,5) and f.cod_tipo_nomina not in (10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,33,37,38,39,40,9) ) `
        );
    //condition = ;

    dep.forEach((element) => {
      const codSplit = element.split("-");
      condition = IS_ONLY_LN
        ? condition.concat(`OR ( f.cod_dep=1 and f.cod_secretaria=${codSplit[0]} and f.cod_direccion=${codSplit[1]} and f.cod_tipo_nomina in (1,2,3) ) `)
        : condition.concat(
            `OR ( f.cod_dep=1 and f.cod_secretaria=${codSplit[0]} and f.cod_direccion=${codSplit[1]} and f.cod_tipo_nomina not in (10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,33,37,38,39,40,9) ) `
          );
      //condition = ;
    });

    condition = IS_ONLY_LN
      ? condition.concat(`OR ( f.cod_dep=1 and f.cod_secretaria=10 and f.cod_direccion NOT IN (8,9) and f.cod_tipo_nomina in (1,2,3) ) `)
      : condition.concat(
          `OR ( f.cod_dep=1 and f.cod_secretaria=10 and f.cod_direccion NOT IN (8,9) and f.cod_tipo_nomina not in (10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,33,37,38,39,40,9) ) `
        );
    //condition = ;

    condition = IS_ONLY_LN
      ? condition.concat(`OR ( f.cod_dep=1 and f.cod_secretaria=13 and f.cod_direccion NOT IN (2,3,4,5,8) and f.cod_tipo_nomina in (1,2,3) ) `)
      : condition.concat(
          `OR ( f.cod_dep=1 and f.cod_secretaria=13 and f.cod_direccion NOT IN (2,3,4,5,8) and f.cod_tipo_nomina not in (10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,33,37,38,39,40,9) ) `
        );
    //condition = ;

    condition = IS_ONLY_LN
      ? condition.concat(`OR ( f.cod_dep=1 and f.cod_secretaria=15 and f.cod_direccion NOT IN (1,2,3,4,5,8,9) and f.cod_tipo_nomina in (1,2,3) ) `)
      : condition.concat(
          `OR ( f.cod_dep=1 and f.cod_secretaria=15 and f.cod_direccion NOT IN (1,2,3,4,5,8,9) and f.cod_tipo_nomina not in (10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,33,37,38,39,40,9) ) `
        );
    //condition = ;

    condition = IS_ONLY_LN
      ? condition.concat(`OR ( f.cod_dep=1 and f.cod_secretaria in (02,03,05,06,07,08,09,11,12,14,16,17,18,19,20,21) and f.cod_tipo_nomina in (1,2,3,29) ) `)
      : condition.concat(
          `OR ( f.cod_dep=1 and f.cod_secretaria in (02,03,05,06,07,08,09,11,12,14,16,17,18,19,20,21) and f.cod_tipo_nomina not in (10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,33,37,38,39,40,9) ) `
        );
    //condition = ;

    condition = IS_ONLY_LN ? condition.concat(`OR ( f.cod_dep=1009 and f.cod_tipo_nomina in (1,7) ) `) : condition.concat(`OR ( f.cod_dep=1009 ) `);
    //condition = ;

    condition = IS_ONLY_LN ? condition.concat(`OR ( f.cod_dep=1014 and f.cod_tipo_nomina in (1,2,3,8) ) `) : condition.concat(`OR ( f.cod_dep=1014 ) `);
    //condition = ;

    condition = IS_ONLY_LN ? condition.concat(`OR ( f.cod_dep=1015 and f.cod_tipo_nomina in (1,2,3) ) `) : condition.concat(`OR ( f.cod_dep=1015 ) `);
    //condition = ;

    condition = IS_ONLY_LN ? condition.concat(`OR ( f.cod_dep in (1039) and f.cod_tipo_nomina in (1) ) `) : condition.concat(`OR ( f.cod_dep in (1039) ) `);
    //condition = ;

    condition = IS_ONLY_LN ? condition.concat(`OR ( f.cod_dep=1040 and f.cod_tipo_nomina in (1,2,3) ) `) : condition.concat(`OR ( f.cod_dep=1040 ) `);
    //condition = ;

    condition = IS_ONLY_LN
      ? condition.concat(
          `OR ( f.cod_dep in (1000,1001,1002,1003,1004,1005,1006,1007,1008,1010,1011,1012,1013,1016,1017,1018,1019,1020,1021,1022,1023,1027,1028,1029,1030,1031,1032,1033,1034,1035,1036,1037,1038,1041,1042,1043,1044,1045,1046) and f.cod_tipo_nomina in (1,2) ) `
        )
      : condition.concat(
          `OR ( f.cod_dep in (1000,1001,1002,1003,1004,1005,1006,1007,1008,1010,1011,1012,1013,1016,1017,1018,1019,1022,1023,1027,1028,1029,1030,1031,1032,1033,1034,1035,1036,1037,1038,1041,1042,1043,1044,1045,1046) ) `
        );
    /*condition = 
    );*/

    const CURRENT_YEAR = new Date().getFullYear();
    const sqlQuery = `SELECT  f.cedula_identidad, f.primer_nombre || ' ' || f.segundo_nombre || ' ' || f.primer_apellido || ' ' || f.segundo_apellido as nombre,
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
    (select denominacion from cnmd06_profesiones where cod_profesion=dp.cod_profesion) as profesion,
    (select denominacion from cnmd06_especialidades where cod_profesion=dp.cod_profesion and cod_especialidad=dp.cod_especialidad) as especialidad,
    f.fecha_ingreso, f.direccion_habitacion, f.telefonos_habitacion, f.carnet,
    (select devolver_grado_puesto(
      (select xy.clasificacion_personal from cnmd01 xy where xy.cod_dep=t.cod_dep and xy.cod_tipo_nomina=t.cod_tipo_nomina), t.cod_puesto) )as cod_grado_puesto,
      hn.cod_tipo_nomina
    FROM v_cnmd06_fichas_2 as f 
    FULL OUTER JOIN cnmd05 as t on f.cod_dep=t.cod_dep and f.cod_ficha=t.cod_ficha and f.cod_cargo=t.cod_cargo and t.cod_tipo_nomina=f.cod_tipo_nomina
    FULL OUTER JOIN cnmd01 as hn on hn.cod_dep=t.cod_dep and hn.cod_tipo_nomina=t.cod_tipo_nomina 
    FULL OUTER JOIN cnmd06_datos_personales as dp on dp.cedula_identidad=f.cedula_identidad 
    where t.ano=${CURRENT_YEAR} and f.condicion_actividad_ficha=1 and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14) [condition_ext] and (${condition}) ORDER BY t.cod_tipo_nomina`;

    const query = await unifiedQuery({ sqlQuery, table: "f." });

    const result = query.map((emp) => {
      return {
        ...emp,
        edad: diffYear(emp.fecha_nacimiento),
      };
    });

    res.json(result);
    return true;
  } catch (error) {
    res.status(500).send({ message: "Error en la consulta unificada", error: error.message });
  }
});

router.get("/hoja_vida/cantidad_empleados", async (req, res) => {
  try {
    const CURRENT_YEAR = new Date().getFullYear();
    const sqlQuery = IS_ONLY_LN
      ? `SELECT  
      COUNT(f.cedula_identidad),
      f.deno_cod_secretaria as denominacion
      FROM v_cnmd06_fichas_2 as f 
      FULL OUTER JOIN cnmd05 as t on f.cod_dep=t.cod_dep and f.cod_ficha=t.cod_ficha and f.cod_cargo=t.cod_cargo and t.cod_tipo_nomina=f.cod_tipo_nomina
      FULL OUTER JOIN cnmd01 as hn on hn.cod_dep=t.cod_dep and hn.cod_tipo_nomina=t.cod_tipo_nomina
      FULL OUTER JOIN cnmd06_datos_personales as dp on dp.cedula_identidad=f.cedula_identidad 
      where t.ano=${CURRENT_YEAR} and f.condicion_actividad_ficha=1 [condition_ext] 
      and f.cod_dep=1 and f.cod_secretaria in (02,03,05,06,07,08,09,11,12,14,16,17,18,19,20,21) and t.cod_tipo_nomina in (1,2,3,29)  
      GROUP BY f.deno_cod_secretaria
      UNION
      SELECT  
      COUNT(f.cedula_identidad),
      f.deno_cod_direccion as denominacion
      FROM v_cnmd06_fichas_2 as f 
      FULL OUTER JOIN cnmd05 as t on f.cod_dep=t.cod_dep and f.cod_ficha=t.cod_ficha and f.cod_cargo=t.cod_cargo and t.cod_tipo_nomina=f.cod_tipo_nomina
      FULL OUTER JOIN cnmd01 as hn on hn.cod_dep=t.cod_dep and hn.cod_tipo_nomina=t.cod_tipo_nomina
      FULL OUTER JOIN cnmd06_datos_personales as dp on dp.cedula_identidad=f.cedula_identidad 
      where t.ano=${CURRENT_YEAR} and f.condicion_actividad_ficha=1 [condition_ext] 
      and (
      ( f.cod_dep=1 and f.cod_secretaria=01 and f.cod_direccion=3 and t.cod_tipo_nomina in (1,2,3) ) OR 
      ( f.cod_dep=1 and f.cod_secretaria=01 and f.cod_direccion=4 and t.cod_tipo_nomina in (1,2,3) ) OR 
      ( f.cod_dep=1 and f.cod_secretaria=01 and f.cod_direccion=5 and t.cod_tipo_nomina in (1,2,3) ) OR 
      ( f.cod_dep=1 and f.cod_secretaria=10 and f.cod_direccion=8 and t.cod_tipo_nomina in (1,2,3) ) OR 
      ( f.cod_dep=1 and f.cod_secretaria=10 and f.cod_direccion=9 and t.cod_tipo_nomina in (1,2,3) ) OR 
      ( f.cod_dep=1 and f.cod_secretaria=13 and f.cod_direccion=2 and t.cod_tipo_nomina in (1,2,3) ) OR 
      ( f.cod_dep=1 and f.cod_secretaria=13 and f.cod_direccion=3 and t.cod_tipo_nomina in (1,2,3) ) OR 
      ( f.cod_dep=1 and f.cod_secretaria=13 and f.cod_direccion=4 and t.cod_tipo_nomina in (1,2,3) ) OR 
      ( f.cod_dep=1 and f.cod_secretaria=13 and f.cod_direccion=5 and t.cod_tipo_nomina in (1,2,3) ) OR 
      ( f.cod_dep=1 and f.cod_secretaria=13 and f.cod_direccion=8 and t.cod_tipo_nomina in (1,2,3) ) OR 
      ( f.cod_dep=1 and f.cod_secretaria=15 and f.cod_direccion=1 and t.cod_tipo_nomina in (1,2,3) ) OR 
      ( f.cod_dep=1 and f.cod_secretaria=15 and f.cod_direccion=2 and t.cod_tipo_nomina in (1,2,3) ) OR 
      ( f.cod_dep=1 and f.cod_secretaria=15 and f.cod_direccion=4 and t.cod_tipo_nomina in (1,2,3) ) OR 
      ( f.cod_dep=1 and f.cod_secretaria=15 and f.cod_direccion=8 and t.cod_tipo_nomina in (1,2,3) ) OR 
      ( f.cod_dep=1 and f.cod_secretaria=15 and f.cod_direccion=9 and t.cod_tipo_nomina in (1,2,3) ) ) 
      GROUP BY f.deno_cod_direccion
      UNION
      SELECT  
      COUNT(f.cedula_identidad),
      f.deno_cod_secretaria as denominacion
      FROM v_cnmd06_fichas_2 as f 
      FULL OUTER JOIN cnmd05 as t on f.cod_dep=t.cod_dep and f.cod_ficha=t.cod_ficha and f.cod_cargo=t.cod_cargo and t.cod_tipo_nomina=f.cod_tipo_nomina
      FULL OUTER JOIN cnmd01 as hn on hn.cod_dep=t.cod_dep and hn.cod_tipo_nomina=t.cod_tipo_nomina
      FULL OUTER JOIN cnmd06_datos_personales as dp on dp.cedula_identidad=f.cedula_identidad 
      where t.ano=${CURRENT_YEAR} and f.condicion_actividad_ficha=1 [condition_ext] 
      and (
      ( f.cod_dep=1 and f.cod_secretaria=1 and f.cod_direccion NOT IN (2,3,4,5) and t.cod_tipo_nomina in (1,2,3) ) OR 
      ( f.cod_dep=1 and f.cod_secretaria=10 and f.cod_direccion NOT IN (8,9) and t.cod_tipo_nomina in (1,2,3) ) OR 
      ( f.cod_dep=1 and f.cod_secretaria=13 and f.cod_direccion NOT IN (2,3,4,5,8) and t.cod_tipo_nomina in (1,2,3) ) OR 
      ( f.cod_dep=1 and f.cod_secretaria=15 and f.cod_direccion NOT IN (1,2,3,4,5,8,9) and t.cod_tipo_nomina in (1,2,3) ) ) 
      GROUP BY f.deno_cod_secretaria`
      : `SELECT  
      COUNT(f.cedula_identidad),
      f.deno_cod_secretaria as denominacion
      FROM v_cnmd06_fichas_2 as f 
      FULL OUTER JOIN cnmd05 as t on f.cod_dep=t.cod_dep and f.cod_ficha=t.cod_ficha and f.cod_cargo=t.cod_cargo and t.cod_tipo_nomina=f.cod_tipo_nomina
      FULL OUTER JOIN cnmd01 as hn on hn.cod_dep=t.cod_dep and hn.cod_tipo_nomina=t.cod_tipo_nomina
      FULL OUTER JOIN cnmd06_datos_personales as dp on dp.cedula_identidad=f.cedula_identidad 
      where t.ano=${CURRENT_YEAR} and f.condicion_actividad_ficha=1 and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14) [condition_ext] 
      and f.cod_dep=1 and f.cod_secretaria in (02,03,05,06,07,08,09,11,12,14,16,17,18,19,20,21)  and f.cod_tipo_nomina not in (10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,33,37,38,39,40,9) 
      GROUP BY f.deno_cod_secretaria
      UNION
      SELECT  
      COUNT(f.cedula_identidad),
      f.deno_cod_direccion as denominacion
      FROM v_cnmd06_fichas_2 as f 
      FULL OUTER JOIN cnmd05 as t on f.cod_dep=t.cod_dep and f.cod_ficha=t.cod_ficha and f.cod_cargo=t.cod_cargo and t.cod_tipo_nomina=f.cod_tipo_nomina
      FULL OUTER JOIN cnmd01 as hn on hn.cod_dep=t.cod_dep and hn.cod_tipo_nomina=t.cod_tipo_nomina
      FULL OUTER JOIN cnmd06_datos_personales as dp on dp.cedula_identidad=f.cedula_identidad 
      where t.ano=${CURRENT_YEAR} and f.condicion_actividad_ficha=1 and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14) and f.cod_tipo_nomina not in (10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,33,37,38,39,40,9) [condition_ext] 
      and (
      ( f.cod_dep=1 and f.cod_secretaria=01 and f.cod_direccion=3 ) OR 
      ( f.cod_dep=1 and f.cod_secretaria=01 and f.cod_direccion=4 ) OR 
      ( f.cod_dep=1 and f.cod_secretaria=01 and f.cod_direccion=5 ) OR 
      ( f.cod_dep=1 and f.cod_secretaria=10 and f.cod_direccion=8 ) OR 
      ( f.cod_dep=1 and f.cod_secretaria=10 and f.cod_direccion=9 ) OR 
      ( f.cod_dep=1 and f.cod_secretaria=13 and f.cod_direccion=2 ) OR 
      ( f.cod_dep=1 and f.cod_secretaria=13 and f.cod_direccion=3 ) OR 
      ( f.cod_dep=1 and f.cod_secretaria=13 and f.cod_direccion=4 ) OR 
      ( f.cod_dep=1 and f.cod_secretaria=13 and f.cod_direccion=5 ) OR 
      ( f.cod_dep=1 and f.cod_secretaria=13 and f.cod_direccion=8 ) OR 
      ( f.cod_dep=1 and f.cod_secretaria=15 and f.cod_direccion=1 ) OR 
      ( f.cod_dep=1 and f.cod_secretaria=15 and f.cod_direccion=2 ) OR 
      ( f.cod_dep=1 and f.cod_secretaria=15 and f.cod_direccion=4 ) OR 
      ( f.cod_dep=1 and f.cod_secretaria=15 and f.cod_direccion=8 ) OR 
      ( f.cod_dep=1 and f.cod_secretaria=15 and f.cod_direccion=9 ) ) 
      GROUP BY f.deno_cod_direccion
      UNION
      SELECT  
      COUNT(f.cedula_identidad),
      f.deno_cod_secretaria as denominacion
      FROM v_cnmd06_fichas_2 as f 
      FULL OUTER JOIN cnmd05 as t on f.cod_dep=t.cod_dep and f.cod_ficha=t.cod_ficha and f.cod_cargo=t.cod_cargo and t.cod_tipo_nomina=f.cod_tipo_nomina
      FULL OUTER JOIN cnmd01 as hn on hn.cod_dep=t.cod_dep and hn.cod_tipo_nomina=t.cod_tipo_nomina
      FULL OUTER JOIN cnmd06_datos_personales as dp on dp.cedula_identidad=f.cedula_identidad 
      where t.ano=${CURRENT_YEAR} and f.condicion_actividad_ficha=1 and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14) and f.cod_tipo_nomina not in (10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,33,37,38,39,40,9) [condition_ext] 
      and (
      ( f.cod_dep=1 and f.cod_secretaria=1 and f.cod_direccion NOT IN (2,3,4,5) ) OR 
      ( f.cod_dep=1 and f.cod_secretaria=10 and f.cod_direccion NOT IN (8,9) ) OR 
      ( f.cod_dep=1 and f.cod_secretaria=13 and f.cod_direccion NOT IN (2,3,4,5,8) ) OR 
      ( f.cod_dep=1 and f.cod_secretaria=15 and f.cod_direccion NOT IN (1,2,3,4,5,8,9) ) ) 
      GROUP BY f.deno_cod_secretaria`;
    /*const sqlQuery = ;*/
    const sqlQuery_dep = IS_ONLY_LN
      ? `SELECT  
      COUNT(f.cedula_identidad),
      f.denominacion_dependencia as denominacion
      FROM v_cnmd06_fichas_2 as f 
      FULL OUTER JOIN cnmd05 as t on f.cod_dep=t.cod_dep and f.cod_ficha=t.cod_ficha and f.cod_cargo=t.cod_cargo and t.cod_tipo_nomina=f.cod_tipo_nomina
      FULL OUTER JOIN cnmd01 as hn on hn.cod_dep=t.cod_dep and hn.cod_tipo_nomina=t.cod_tipo_nomina
      FULL OUTER JOIN cnmd06_datos_personales as dp on dp.cedula_identidad=f.cedula_identidad 
      where t.ano=${CURRENT_YEAR} and f.condicion_actividad_ficha=1 and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14) [condition_ext] 
      and (
      ( f.cod_dep=1009 ) OR 
      ( f.cod_dep=1014 ) OR 
      ( f.cod_dep=1015 ) OR 
      ( f.cod_dep in (1039) ) OR 
      ( f.cod_dep=1040 ) OR 
      ( f.cod_dep in (1000,1001,1002,1003,1004,1005,1006,1007,1008,1010,1011,1012,1013,1016,1017,1018,1019,1020,1021,1022,1023,1027,1028,1029,1030,1031,1032,1033,1034,1035,1036,1037,1038,1041,1042,1043,1044,1045,1046) ) ) 
      GROUP BY f.denominacion_dependencia`
      : `SELECT  
      COUNT(f.cedula_identidad),
      f.denominacion_dependencia as denominacion
      FROM v_cnmd06_fichas_2 as f 
      FULL OUTER JOIN cnmd05 as t on f.cod_dep=t.cod_dep and f.cod_ficha=t.cod_ficha and f.cod_cargo=t.cod_cargo and t.cod_tipo_nomina=f.cod_tipo_nomina
      FULL OUTER JOIN cnmd01 as hn on hn.cod_dep=t.cod_dep and hn.cod_tipo_nomina=t.cod_tipo_nomina
      FULL OUTER JOIN cnmd06_datos_personales as dp on dp.cedula_identidad=f.cedula_identidad 
      where t.ano=${CURRENT_YEAR} and f.condicion_actividad_ficha=1 [condition_ext] 
      and (
      ( f.cod_dep=1009 and f.cod_tipo_nomina in (1,7) ) OR 
      ( f.cod_dep=1014 and f.cod_tipo_nomina in (1,2,3,8) ) OR 
      ( f.cod_dep=1015 and f.cod_tipo_nomina in (1,2,3) ) OR 
      ( f.cod_dep in (1039) and f.cod_tipo_nomina in (1) ) OR 
      ( f.cod_dep=1040 and f.cod_tipo_nomina in (1,2,3) ) OR 
      ( f.cod_dep in (1000,1001,1002,1003,1004,1005,1006,1007,1008,1010,1011,1012,1013,1016,1017,1018,1019,1020,1021,1022,1023,1027,1028,1029,1030,1031,1032,1033,1034,1035,1036,1037,1038,1041,1042,1043,1044,1045,1046) and f.cod_tipo_nomina in (1,2) ) ) 
      GROUP BY f.denominacion_dependencia`;

    const query = await unifiedQuery({ sqlQuery, table: "f." });
    const query_dep = await unifiedQuery({ sqlQuery: sqlQuery_dep, table: "f." });
    res.json({ centralizados: query, descentralizados: query_dep });
    return true;
  } catch (error) {
    res.status(500).send({ message: "Error en la consulta unificada", error: error.message });
  }
});

router.get("/sisap/lista_dep/", async (req, res) => {
  try {
    const sqlQueryDep = `SELECT cod_dep, denominacion FROM arrd05 WHERE cod_dep>=1000 and cod_dep not in (1020,1024,1025,1026) ORDER BY cod_dep`;
    const sqlQuerySec = `SELECT DISTINCT (
      CASE 
        WHEN c.cod_secretaria < '10' 
        THEN ('0' || c.cod_secretaria || '-00')::varchar 
        ELSE (c.cod_secretaria || '-00')::varchar 
      END)::varchar as cod_dep,
          ( SELECT xc.denominacion
          FROM cugd02_secretaria xc
          WHERE xc.cod_tipo_institucion = c.cod_tipo_inst AND xc.cod_institucion = c.cod_inst AND 
                xc.cod_dependencia = c.cod_dep AND xc.cod_dir_superior = c.cod_dir_superior AND 
                xc.cod_coordinacion = c.cod_coordinacion AND xc.cod_secretaria = c.cod_secretaria
          GROUP BY xc.denominacion) AS denominacion
        FROM cnmd05 c where cod_dep=1 and cod_ficha!=0
        UNION
        SELECT
          DISTINCT (
            CASE 
              WHEN cod_secretaria < '10' 
              THEN ('0' || cod_secretaria || '-' || cod_direccion)::varchar 
              ELSE (cod_secretaria || '-' || cod_direccion)::varchar
            END
          )::varchar as cod_dep,
          denominacion
        FROM cugd02_direccion
          WHERE cod_dependencia=1 AND cod_coordinacion=1 AND ( (cod_secretaria=1 AND cod_direccion in (3,5)) OR (cod_secretaria=10 AND cod_direccion in (8,9)) OR (cod_secretaria=13 AND cod_direccion in (2,3,4,5,8)) OR (cod_secretaria=15 and cod_direccion in (1,2,4,8,9)))
          ORDER BY cod_dep
         `;

    const queryDep = await specificQuery({ sqlQuery: sqlQueryDep, db: 1 });
    const querySec = await specificQuery({ sqlQuery: sqlQuerySec, db: 1 });
    const result = querySec.concat(queryDep);

    res.json(result);
    return true;
  } catch (error) {
    res.status(500).send({ message: "Error en la consulta unificada", error: error.message });
  }
});

export default router;
