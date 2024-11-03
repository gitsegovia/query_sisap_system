import express from "express";
import unifiedQuery, { identifiedQuery, specificQuery } from "./sequelizedb";

const IS_ONLY_LN = false;
const DEP_ENTE = [1006, 1021, 1027, 1028, 1036, 1037, 1038, 1039, 1040, 1042, 1043, 1045, 1046, 1047];

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
      ? `f.cedula_identidad=${cedula} and t.ano=${CURRENT_YEAR} and f.condicion_actividad_ficha=1 [condition_ext]`
      : `f.cedula_identidad = ${cedula} and t.ano = ${CURRENT_YEAR} and f.condicion_actividad_ficha = 1 [condition_ext]`;
    // const where = IS_ONLY_LN
    //   ? `f.cedula_identidad=${cedula} and t.ano=${CURRENT_YEAR} and f.condicion_actividad_ficha=1 and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14) [condition_ext]`
    //   : `f.cedula_identidad = ${cedula} and t.ano = ${CURRENT_YEAR} and f.condicion_actividad_ficha = 1 [condition_ext]`;

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
      ? `f.cedula_identidad=${cedula} and t.ano=${CURRENT_YEAR} and f.condicion_actividad_ficha=1 [condition_ext]`
      : `f.cedula_identidad=${cedula} and t.ano=${CURRENT_YEAR} and f.condicion_actividad_ficha=1 [condition_ext]`;
    // const where = IS_ONLY_LN
    //   ? `f.cedula_identidad=${cedula} and t.ano=${CURRENT_YEAR} and f.condicion_actividad_ficha=1 and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14) [condition_ext]`
    //   : `f.cedula_identidad=${cedula} and t.ano=${CURRENT_YEAR} and f.condicion_actividad_ficha=1 and hn.clasificacion_personal in (1,17,18) [condition_ext]`;

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
        : `f.cod_dep=${cod_dep} and f.cod_tipo_nomina not in (7,8,9,10,11,12) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`;
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
        : `f.cod_dep=${cod_dep} and f.cod_tipo_nomina not in (9,10,11) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`;
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
        : `f.cod_dep=${cod_dep} and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)`;
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
      return IS_ONLY_LN ? `f.cod_dep=${cod_dep} and f.cod_tipo_nomina in (1,2) and hn.clasificacion_personal not in (7,8,13,3,4,6,15,9,10,11,12,13,14)` : ``;
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
    (select denominacion from cnmd06_profesiones where cod_profesion=dp.cod_profesion) as profesion,
    (select denominacion from cnmd06_especialidades where cod_profesion=dp.cod_profesion and cod_especialidad=dp.cod_especialidad) as especialidad,
    f.fecha_ingreso, f.direccion_habitacion, f.telefonos_habitacion, f.carnet,
    (select devolver_grado_puesto(
      (select xy.clasificacion_personal from cnmd01 xy where xy.cod_dep=t.cod_dep and xy.cod_tipo_nomina=t.cod_tipo_nomina), t.cod_puesto) )as cod_grado_puesto,
      hn.cod_tipo_nomina, hn.denominacion
    FROM v_cnmd06_fichas_2 as f 
    FULL OUTER JOIN cnmd05 as t on f.cod_dep=t.cod_dep and f.cod_ficha=t.cod_ficha and f.cod_cargo=t.cod_cargo and t.cod_tipo_nomina=f.cod_tipo_nomina
    FULL OUTER JOIN cnmd01 as hn on hn.cod_dep=t.cod_dep and hn.cod_tipo_nomina=t.cod_tipo_nomina 
    FULL OUTER JOIN cnmd06_datos_personales as dp on dp.cedula_identidad=f.cedula_identidad 
    where f.cedula_identidad not in(17272487, 3121415,3516898,3953350,4285183,4877107,5008142,5621980,5622394,6345849,6405730,6626232,6626277,6627482,6641554,6940025,7278034,7279615,7292652,7295995,7947144,8173284,8413995,8552594,8570155,8574193,8620863,8621316,8627134,8783227,8783879,8784457,8784657,8786434,8789992,8791843,8798328,8799042,8803322,8803363,8909143,8996101,8999871,9591223,9596950,9875740,9883031,9883515,9886234,9889227,10266922,10269651,10273815,10665103,10667050,10670434,10980752,11053128,11115431,11120282,11237678,11367254,11369489,11757562,11759500,11795566,11796841,11844546,12119137,12322982,12323559,12410854,12512086,12897477,13152298,13153437,13237082,13794978,13860220,14344452,14520381,14538971,14539214,14693507,14871624,14948026,15152257,15248588,15301671,15452027,15452574,15480321,15681918,15710817,16383483,16506971,16511064,16790345,16913325,17272513,17353679,17746355,17936870,18328167,18543178,18543198,18894872,19019256,19067800,19100721,19246030,19405492,19473610,19657990,19724913,19959783,19986482,20184489,20247847,20588884,20876101,21177638,21581339,21658471,23647339,24792900,25419847,27238440,30811163,3167720,6800718,20528474,8630884,11236283,10499354,7291076,16144933,15248175,8790968,10983518,10497643,14294391,12512650,12899227,12475710,9918931,14672586,16506165,17001528,13680322,9874646,8791481,6997740,12117664,8630784,22614807,16383567,22882050,12766040,16504587,11369286,16236363,8558331,18067410,8805348,5072098,11842698,15100091,19759639,10496825,17374212,11368831,18376350,12582770,13482691,14191166,15811760,4345626,8728909,19725594,24678325,82050877,9883003,4545380,27631216,8151814,8556221,10855142,13874018,16075260,16803261,17166165,19222422,19724480,19948836,24976488,2517455,3581077,5153837,5157088,6450664,9889039,9889428,12840505,12841010,2516732,4388634,5430469,6002161,7281697,7294482,8995885,8996437,11116812,14146614,10981600,13571432,20878323,9886484,9888274,11124321,12842961,13622436,14870473,15710663,21147972,3951934,3958684,4284675,4391053,4798843,5157359,7275674,7276672,8554666,8784662,9886011,9888135,10668280,10670925,10671144,11090574,14645308,14870019,16912812,17513856,26081962,27238537,5523033,19725353) and ${condition_dep} and t.ano=${CURRENT_YEAR} and f.condicion_actividad_ficha=1 [condition_ext] ORDER BY t.cod_tipo_nomina`;

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
          `( f.cod_dep=1 and f.cod_secretaria=1 and f.cod_direccion NOT IN (2,3,4,5) and f.cod_tipo_nomina not in (10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,33,37,38,39,40,9,6) ) `
        );
    //condition = ;

    dep.forEach((element) => {
      const codSplit = element.split("-");
      condition = IS_ONLY_LN
        ? condition.concat(`OR ( f.cod_dep=1 and f.cod_secretaria=${codSplit[0]} and f.cod_direccion=${codSplit[1]} and f.cod_tipo_nomina in (1,2,3) ) `)
        : condition.concat(
            `OR ( f.cod_dep=1 and f.cod_secretaria=${codSplit[0]} and f.cod_direccion=${codSplit[1]} and f.cod_tipo_nomina not in (10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,33,37,38,39,40,9,6) ) `
          );
      //condition = ;
    });

    condition = IS_ONLY_LN
      ? condition.concat(`OR ( f.cod_dep=1 and f.cod_secretaria=10 and f.cod_direccion NOT IN (8,9) and f.cod_tipo_nomina in (1,2,3) ) `)
      : condition.concat(
          `OR ( f.cod_dep=1 and f.cod_secretaria=10 and f.cod_direccion NOT IN (8,9) and f.cod_tipo_nomina not in (10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,33,37,38,39,40,9,6) ) `
        );
    //condition = ;

    condition = IS_ONLY_LN
      ? condition.concat(`OR ( f.cod_dep=1 and f.cod_secretaria=13 and f.cod_direccion NOT IN (2,3,4,5,8) and f.cod_tipo_nomina in (1,2,3) ) `)
      : condition.concat(
          `OR ( f.cod_dep=1 and f.cod_secretaria=13 and f.cod_direccion NOT IN (2,3,4,5,8) and f.cod_tipo_nomina not in (10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,33,37,38,39,40,9,6) ) `
        );
    //condition = ;

    condition = IS_ONLY_LN
      ? condition.concat(`OR ( f.cod_dep=1 and f.cod_secretaria=15 and f.cod_direccion NOT IN (1,2,3,4,5,8,9) and f.cod_tipo_nomina in (1,2,3) ) `)
      : condition.concat(
          `OR ( f.cod_dep=1 and f.cod_secretaria=15 and f.cod_direccion NOT IN (1,2,3,4,5,8,9) and f.cod_tipo_nomina not in (10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,33,37,38,39,40,9,6) ) `
        );
    //condition = ;

    condition = IS_ONLY_LN
      ? condition.concat(`OR ( f.cod_dep=1 and f.cod_secretaria in (02,03,05,06,07,08,09,11,12,14,16,17,18,19,20,21) and f.cod_tipo_nomina in (1,2,3,29) ) `)
      : condition.concat(
          `OR ( f.cod_dep=1 and f.cod_secretaria in (02,03,05,06,07,08,09,11,12,14,16,17,18,19,20,21) and f.cod_tipo_nomina not in (10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,33,37,38,39,40,9,6) ) `
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

router.get("/fichas/consulta/:cedula", async (req, res) => {
  const { cedula } = req.params;

  if (!cedula) {
    res.status(404).send("Cedula requerida");
    return false;
  }

  try {
    let valid = false;
    let db = 1;
    let beneficiario = "";

    const sqlQuery = `SELECT 
 fb.cedula_identidad, dp.primer_nombre || ' ' || dp.segundo_nombre || ' ' || dp.primer_apellido || ' ' || dp.segundo_apellido as nombre, 
    fb.cod_dep,(SELECT denominacion from arrd05 where cod_dep=fb.cod_dep) as dependencia, fb.cod_tipo_nomina, hn.denominacion, fb.cod_cargo, fb.cod_ficha, f.condicion_actividad, h.cod_puesto, f.fecha_ingreso, f.fecha_movimiento,
    ( SELECT devolver_denominacion_puesto(( SELECT xy.clasificacion_personal FROM cnmd01 xy
      WHERE xy.cod_dep = fb.cod_dep AND xy.cod_tipo_nomina = fb.cod_tipo_nomina), h.cod_puesto) AS devolver_denominacion_puesto) AS demonimacion_puesto
    FROM (SELECT cod_dep, cod_tipo_nomina,
       cod_cargo, cod_ficha, cedula_identidad
  FROM cnmd08_historia_trabajador
  where cedula_identidad= ${cedula}
  group by cod_dep, cod_tipo_nomina, 
       cod_cargo, cod_ficha, cedula_identidad) as fb
    LEFT JOIN cnmd06_fichas as f ON f.cod_ficha=fb.cod_ficha and f.cod_cargo=fb.cod_cargo and f.cedula_identidad=fb.cedula_identidad
    LEFT JOIN cnmd05 as h ON h.cod_dep=fb.cod_dep and h.cod_tipo_nomina=fb.cod_tipo_nomina and h.cod_cargo=fb.cod_cargo
    FULL OUTER JOIN cnmd01 as hn on hn.cod_dep=fb.cod_dep and hn.cod_tipo_nomina=fb.cod_tipo_nomina
    FULL OUTER JOIN cnmd06_datos_personales as dp on dp.cedula_identidad=fb.cedula_identidad 
    where fb.cedula_identidad =${cedula} [condition_ext]`;

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

router.get("/fichas/consulta_expediente/:cedula/:cod_dep", async (req, res) => {
  const { cedula, cod_dep } = req.params;

  if (!cedula) {
    res.status(404).send("Cedula requerida");
    return false;
  }

  let db = 1;

  if (DEP_ENTE.includes(cod_dep)) {
    db = 2;
  }

  if (cod_dep == 1035) {
    db = 3;
  }

  if (cod_dep == 1041) {
    db = 4;
  }

  try {
    const sqlQuery_exp_adm = `SELECT 
 cargo_desempenado,
  entidad_federal,
  fecha_ingreso,
  fecha_egreso
  FROM cnmd06_datos_experiencia_administrativa
  where cedula=${cedula} order by fecha_ingreso DESC`;

    const query_exp_adm = await specificQuery({ sqlQuery: sqlQuery_exp_adm, db });

    const sqlQuery_formacion_prof = `SELECT 
 deno_curso, deno_institucion, duracion, desde, hasta, observaciones
  FROM v_cnmd06_datos_formacion_profesional
  where cedula=${cedula} order by desde DESC`;

    const query_formacion_prof = await specificQuery({ sqlQuery: sqlQuery_formacion_prof, db });

    const sqlQuery_educativos = `SELECT 
 deno_nivel, deno_institucion, fecha_inicio, fecha_culminacion, observaciones
  FROM v_cnmd06_datos_educativos
  where cedula=${cedula} order by fecha_inicio DESC`;

    const query_educativos = await specificQuery({ sqlQuery: sqlQuery_educativos, db });

    res.json({
      expediente_administrativo: query_exp_adm,
      formacion_profesional: query_formacion_prof,
      formacion_educativa: query_educativos,
    });
    return true;
  } catch (error) {
    res.status(500).send({ message: "Error en la consulta unificada", error: error.message });
  }
});

//RUTAS INTERNAS SISAP ENTE A SISAP PRINCIPAL

router.get("/sisap/solicitud_recurso/partidas/:ano/:cod_sector/:cod_programa/:cod_sub_prog", async (req, res) => {
  try {
    //const { cod_sector, cod_ } = req.params;
    const { ano, cod_sector, cod_programa, cod_sub_prog } = req.params;

    const db = 1;
    const sqlQuery = `SELECT cf.cod_dep, cf.ano, cf.cod_sector, cf.cod_programa, cf.cod_sub_prog, cf.cod_activ_obra, 
       cf.cod_partida, cf.cod_generica, cf.cod_especifica, cf.cod_sub_espec, cf.cod_auxiliar, 
       (cf.asignacion_anual+cf.aumento_traslado_anual+cf.credito_adicional_anual+cf.nacionales_anual-cf.disminucion_traslado_anual-cf.rebaja_anual) as asignacion,
       cf.compromiso_anual, 
       (cf.asignacion_anual+cf.aumento_traslado_anual+cf.credito_adicional_anual+cf.nacionales_anual-cf.disminucion_traslado_anual-cf.rebaja_anual-cf.compromiso_anual) as disponible,
       vcf.deno_activ_obra, vcf.deno_partida, vcf.deno_auxiliar 
  FROM cfpd05 as cf 
  INNER JOIN v_cfpd05_denominaciones as vcf ON cf.cod_dep=vcf.cod_dep AND cf.ano=vcf.ano AND cf.cod_sector=vcf.cod_sector 
             AND cf.cod_programa=vcf.cod_programa AND cf.cod_sub_prog=vcf.cod_sub_prog AND cf.cod_activ_obra=vcf.cod_activ_obra 
             AND cf.cod_partida=vcf.cod_partida AND cf.cod_generica=vcf.cod_generica AND cf.cod_especifica=vcf.cod_especifica 
             AND cf.cod_sub_espec=vcf.cod_sub_espec AND cf.cod_auxiliar=vcf.cod_auxiliar 
  WHERE cf.cod_dep=1 AND cf.cod_partida=407 AND cf.ano=${ano} AND cf.cod_sector=${cod_sector} AND cf.cod_programa=${cod_programa} AND cf.cod_sub_prog=${cod_sub_prog}`;

    const query = await specificQuery({ sqlQuery: sqlQuery, db });

    res.json({
      query,
    });
  } catch (error) {
    res.status(500).send({ message: "Error en la consulta unificada", error: error.message });
  }
});

router.post("/sisap/solicitud_recurso/guardar", express.urlencoded({ extended: true }), async (req, res) => {
  try {
    //13-1-2-53-407-1-3-7-12
    /**
      ano
      numero_documento
      tipo_documento
      fecha_documento
      tipo_recurso
      condicion_juridica
      concepto
      beneficiario
      concepto
      condicion_documento
      num_asiento
      fecha_proceso_registro
      fecha_proceso_anulacion
     */
    //const { rif, beneficiario, concepto, monto_total } = req.body;
    /**
      {
        ano: '2024',
        numero_solicitud: '02',
        fecha_solicitud: '03/11/2024',
        deno_dependencia: 'FUNDACIÓN PATRIA SOCIALISTA (FPS)',
        denominacion_activ_1: 'GESTIÓN ADMINISTRATIVA DE LA FUNDACIÓN PATRIA SOCIALISTA',
        partida_1: '13-1-2-51-407-1-3-7-1',
        denominacion_1: 'FUNDACIÓN PATRIA SOCIALISTA ',
        monto_asignado_1: '502.000,00',
        monto_solicitado_1: '125.499,99',
        monto_disponible_1: '376.500,01',
        monto_solicitar_1: '10,00',
        denominacion_activ_2: 'PATRIA SOCIALISTA (PERSONAL EN PUESTO NO PERMANENTE)',
        partida_2: '13-1-2-52-407-3-7-0-1',
        denominacion_2: '',
        monto_asignado_2: '5.537.881,00',
        monto_solicitado_2: '0,00',
        monto_disponible_2: '5.537.881,00',
        monto_solicitar_2: '10,00',
        denominacion_activ_3: 'PATRIA SOCIALISTA (FCI)',
        partida_3: '13-1-2-53-407-1-3-7-12',
        denominacion_3: 'REHABILITACIÓN DE LA FACHADA DEL HOSPITAL DR RAFAEL ZAMORA AREVALO, PARROQUIA VALLE DE LA PASCUA ESTADO BOLIVIARIANO DE GUARICO ',
        monto_asignado_3: '2.176.872,65',
        monto_solicitado_3: '2.176.827,65',
        monto_disponible_3: '45,00',
        monto_solicitar_3: '0',
        denominacion_activ_4: 'PATRIA SOCIALISTA (PROYECTOS ORDINARIO / PROYECTOS ESTRATEGICOS)',
        partida_4: '13-1-2-54-407-1-3-7-1',
        denominacion_4: 'FUNDACIÓN PATRIA SOCIALISTA (PROYECTOS ORDINARIOS / PROYECTOS ESTRATEGICOS)',
        monto_asignado_4: '21.111.188,33',
        monto_solicitado_4: '21.111.002,99',
        monto_disponible_4: '185,34',
        monto_solicitar_4: '10,00',
        index: '4',
        concepto: 'KKKKK',
        salir: ''
      }

     */
    const { rif, concepto, deno_dependencia, monto_solicitud, id_send } = req.body;

    const db = 0;

    const current = new Date();
    const ano = current.getFullYear();
    const fecha_documento = current.getFullYear() + "-" + current.getMonth() + "-" + current.getDate();
    const concepto_cuerpo = "BENEFICIARIO: " + deno_dependencia + " POR CONCEPTO DE: " + concepto;
    const monto_total = monto_solicitud.replace(".", "").replace(".", ",");

    const sqlQuery_numero = `SELECT numero_compromiso FROM cepd01_compromiso_numero WHERE cod_dep=1 AND ano_compromiso=${ano} AND situacion=1 ORDER BY numero_compromiso ASC LIMIT 1`;

    const res_numero = await specificQuery({ sqlQuery: sqlQuery_numero, db });

    if (res_numero.length == 0) {
      res.status(500).send({ message: "No existe numero de compromiso disponible" });
      return false;
    }

    let numero_compromiso = res_numero[0].numero_compromiso;

    await specificQuery({ sqlQuery: `UPDATE cepd01_compromiso_numero SET situacion=2 WHERE cod_dep=1 AND ano_compromiso=${ano} AND numero_compromiso=${numero_compromiso}`, db });

    const camposT2 =
      "cod_presi,cod_entidad,cod_tipo_inst,cod_inst,cod_dep,ano_documento,numero_documento,cod_tipo_compromiso,fecha_documento,tipo_recurso,rif,cedula_identidad,cod_dir_superior,cod_coordinacion,cod_secretaria,cod_direccion,concepto,monto,condicion_actividad,dia_asiento_registro,mes_asiento_registro,ano_asiento_registro, numero_asiento_registro,username_registro,ano_anulacion,numero_anulacion,dia_asiento_anulacion,mes_asiento_anulacion,ano_asiento_anulacion,numero_asiento_anulacion,username_anulacion,ano_orden_pago,numero_orden_pago,beneficiario,condicion_juridica,fecha_proceso_registro,fecha_proceso_anulacion";

    const sqlQuery_i_cuerpo = `BEGIN SISAP_COMPROMISO; INSERT INTO cepd01_compromiso_cuerpo (${camposT2}) VALUES (1,12,30,12,1,${ano},${numero_compromiso},114,'${fecha_documento}',1,'${rif}','0',1,1,1,1,'${concepto_cuerpo}',${monto_total},1,0,0,0,0,'AUTOADMIN',0,0,'0',0,0,0,0,0,0,'${deno_dependencia}',2,'${fecha_documento}','1900-01-01')`;

    //await specificQuery({ sqlQuery: sqlQuery_i_cuerpo, db });

    const camposT3 =
      "cod_presi,cod_entidad,cod_tipo_inst,cod_inst,cod_dep,ano_documento,numero_documento,ano,cod_sector,cod_programa,cod_sub_prog,cod_proyecto,cod_activ_obra,cod_partida,cod_generica,cod_especifica,cod_sub_espec,cod_auxiliar,monto,numero_control_compromiso";

    let str_mes = "";
    switch (current.getMonth()) {
      case 1:
        str_mes = "ene";
        break;
      case 2:
        str_mes = "feb";
        break;
      case 3:
        str_mes = "mar";
        break;
      case 4:
        str_mes = "abr";
        break;
      case 5:
        str_mes = "may";
        break;
      case 6:
        str_mes = "jun";
        break;
      case 7:
        str_mes = "jul";
        break;
      case 8:
        str_mes = "ago";
        break;
      case 9:
        str_mes = "sep";
        break;
      case 10:
        str_mes = "oct";
        break;
      case 11:
        str_mes = "nov";
        break;

      default:
        str_mes = "dic";
        break;
    }
    $compromiso_mes = "compromiso_" + str_mes;

    let values = "";
    let sql_update132 = "";
    let sql_insert_cfpd21 = "";
    for (let index = 1; index <= id_send; index++) {
      const part_split = req.body["partida_" + index].split("-");
      const monto_partida = req.body["monto_solicitar_" + index].replace(".", "").replace(".", ",");

      const cod_sector = part_split[0];
      const cod_programa = part_split[1];
      const cod_sub_prog = part_split[2];
      const cod_proyecto = 0;
      const cod_activ_obra = part_split[3];
      const cod_partida = part_split[4];
      const cod_generica = part_split[5];
      const cod_especifica = part_split[6];
      const cod_sub_espec = part_split[7];
      let cod_auxiliar = 0;
      if (part_split.length > 8) {
        cod_auxiliar = part_split[8];
      }
      values += `(1,12,30,12,1,${ano},${numero_compromiso},${ano},${cod_sector},${cod_programa},${cod_sub_prog},${cod_proyecto},${cod_activ_obra},${cod_partida},${cod_generica},${cod_especifica},${cod_sub_espec},${cod_auxiliar},${monto_partida},0)`;
      if (index < id_send) {
        values += ",";
      } else {
        values += ";";
      }

      //MOTOR PRESUPUESTARIO
      sql_update132 += `UPDATE cfpd05 set compromiso_anual=compromiso_anual+${monto_partida}, ${compromiso_mes}=${compromiso_mes}+${monto_partida} WHERE cod_dep=1 and ano=${ano} and cod_sector=${cod_sector} and cod_programa=${cod_programa} and cod_sub_prog=${cod_sub_prog} and cod_activ_obra=${cod_activ_obra} and cod_partida=${cod_partida} and cod_generica=${cod_generica} and cod_especifica=${cod_especifica} and cod_sub_espec=${cod_sub_espec} and cod_auxiliar=${cod_auxiliar};`;

      const ccp = `REGISTRO DE COMPROMISO AÑO: ${ann}, NUMERO: ${numero_compromiso} DE FECHA: ${fecha_documento}, ${concepto_cuerpo}`;
      sql_insert_cfpd21 += `INSERT INTO cfpd21 (cod_presi, cod_entidad, cod_tipo_inst, cod_inst, cod_dep, ano, cod_sector, cod_programa, cod_sub_prog, cod_proyecto, cod_activ_obra, cod_partida, cod_generica, cod_especifica, cod_sub_espec, cod_auxiliar, numero_asiento_compromiso, monto, fecha, concepto) VALUES(1,12,30,12,1,${ano},${cod_sector},${cod_programa},${cod_sub_prog},0,${cod_activ_obra},${cod_partida},${cod_generica},${cod_especifica},${cod_sub_espec},${cod_auxiliar},${numero_compromiso},${monto_partida},'${fecha_documento}','${ccp}');`;
    }
    const sqlQuery_i_partidas = `INSERT INTO cepd01_compromiso_partidas (${camposT3}) VALUES ${values}`;
    const sqlQuery_i_update_cfpd05 = sql_update132;
    const sqlQuery_i_insert_cfpd21 = sql_insert_cfpd21;

    res.json({
      numero_compromiso,
      sqlQuery_i_cuerpo,
      sqlQuery_i_partidas,
      sqlQuery_i_update_cfpd05,
      sqlQuery_i_insert_cfpd21,
    });
  } catch (error) {
    res.status(500).send({ message: "Error en la consulta unificada", error: error.message });
  }
});

export default router;
