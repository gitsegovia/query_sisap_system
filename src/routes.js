import express from "express";
import path from "path";
import unifiedQuery, { identifiedQuery, specificQuery } from './sequelizedb';

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
router.get('/fusamiebg/consulta/:cedula?', async (req, res) => {
  const { cedula } = req.params;

  if(!cedula){
    res.status(404).send('Cedula requerida');
    return false;
  }

  try {
    const VALID_DAYS = 30;
    let valid = false;
    let db = 1;
    let periodo_desde ='';
    let beneficiario= '';
    const CURRENT_YEAR = new Date().getFullYear();
    const sqlQuery = `SELECT cedula_identidad, primer_nombre || ' ' || segundo_nombre || ' ' || primer_apellido || ' ' || segundo_apellido as nombre, f.deno_cod_secretaria, f.deno_cod_direccion, f.demonimacion_puesto, f.cod_dep, f.cod_ficha, f.fecha_nacimiento, f.sexo, f.grupo_sanguineo, f.fecha_ingreso, f.direccion_habitacion, f.telefonos_habitacion, f.carnet, hn.periodo_desde FROM v_cnmd06_fichas_2 as f inner join cnmd08_historia_transacciones as t on f.cod_ficha=t.cod_ficha and f.cod_cargo=t.cod_cargo inner join cnmd08_historia_nomina as hn on hn.cod_dep=t.cod_dep and hn.cod_tipo_nomina=t.cod_tipo_nomina and hn.numero_nomina=t.numero_nomina and hn.ano=t.ano where f.cedula_identidad=${cedula} and t.ano=${CURRENT_YEAR} and t.cod_transaccion=103 [condition_ext] order by hn.periodo_desde DESC LIMIT 1`; 
    
    const query = await identifiedQuery({sqlQuery, table: 't.'});
    const checkQuery = Object.values(query).reduce((acc, current) => acc+current.length,0)
    
    if(checkQuery>0){
      const current = new Date();

      if(query.result_db1.length>0){
        const date = new Date(query.result_db1[0].periodo_desde);
        const diffInTime = current.getTime() - date.getTime();
        const diffInDays = Math.ceil(diffInTime / (1000 * 3600 * 24))
        valid = diffInDays<= VALID_DAYS;
        beneficiario=query.result_db1[0];
        periodo_desde= date;
      }else if(query.result_db2.length>0){
        const date = new Date(query.result_db2[0].periodo_desde);
        const diffInTime = current.getTime() - date.getTime();
        const diffInDays = Math.ceil(diffInTime / (1000 * 3600 * 24))
        
        valid = diffInDays<= VALID_DAYS;
        beneficiario=query.result_db2[0];
        periodo_desde= date;
        db=2
      }else if(query.result_db3.length>0){
        const date = new Date(query.result_db3[0].periodo_desde);
        const diffInTime = current.getTime() - date.getTime();
        const diffInDays = Math.ceil(diffInTime / (1000 * 3600 * 24))
        
        valid = diffInDays<= VALID_DAYS;
        beneficiario=query.result_db3[0];
        periodo_desde= date;
        db=3
      }else if(query.result_db4.length>0){
        const date = new Date(query.result_db4[0].periodo_desde);
        const diffInTime = current.getTime() - date.getTime();
        const diffInDays = Math.ceil(diffInTime / (1000 * 3600 * 24))
        
        valid = diffInDays<= VALID_DAYS;
        beneficiario=query.result_db4[0];
        periodo_desde= date;
        db=4
      }      
    }else{
      const sqlQuery_obrero = `SELECT cedula_identidad, primer_nombre || ' ' || segundo_nombre || ' ' || primer_apellido || ' ' || segundo_apellido as nombre, 
      f.deno_cod_secretaria, f.deno_cod_direccion, f.demonimacion_puesto, f.cod_dep, f.cod_ficha, f.fecha_nacimiento, f.sexo, f.grupo_sanguineo, 
      f.fecha_ingreso, f.direccion_habitacion, f.telefonos_habitacion, f.carnet FROM v_cnmd06_fichas_2 as f inner join cnmd05 as t on f.cod_ficha=t.cod_ficha and f.cod_cargo=t.cod_cargo inner join cnmd01 as hn on hn.cod_dep=t.cod_dep and hn.cod_tipo_nomina=t.cod_tipo_nomina where f.cedula_identidad=${cedula} and t.ano=${CURRENT_YEAR} and f.condicion_actividad_ficha=1 and hn.clasificacion_personal in (2,8,10) [condition_ext]`; 
      const query_obrero = await identifiedQuery({sqlQuery: sqlQuery_obrero, table: 't.'});
      
      const checkQuery = Object.values(query_obrero).reduce((acc, current) => acc+current.length,0)
      

      if(checkQuery>0){
        valid=true;
        periodo_desde= new Date();
        if(query_obrero.result_db1.length>0){
          beneficiario=query.result_db1[0];
          db=1;
        }else if(query_obrero.result_db2.length>0){
          beneficiario=query.result_db2[0];
          db=2;
        }else if(query_obrero.result_db3.length>0){
          beneficiario=query.result_db3[0];
          db=3;
        }else if(query_obrero.result_db4.length>0){
          beneficiario=query.result_db4[0];
          db=4;
        }      
      }
    }

    if(valid){
      const sqlQuery_hijos = `SELECT * FROM cnmd06_datos_hijos WHERE cedula=${cedula}`
      const query_hijos = await specificQuery({sqlQuery: sqlQuery_hijos, db});
            
      const sqlQuery_familiares = `SELECT * FROM cnmd06_datos_familiares WHERE cedula=${cedula} and cod_parentesco in (3,4,5,6,7,8)`
      const query_familiares = await specificQuery({sqlQuery: sqlQuery_familiares, db});
            
      const result_employee = {
        beneficiario: beneficiario,
        afiliacion: true,
        hijos: [...query_hijos],
        datos_familiares: [...query_familiares],
        fecha_ultimo_pago: periodo_desde
      };
      res.json(result_employee);
      return true;
    }else{
      res.status(404).send({message: 'No existe resultados', error: ''})
      return false;
    }

  } catch (error) {
    res.status(500).send({message: 'Error en la consulta unificada', error: error.message});
  }
})

router.get('/carnet/consulta/:cedula?', async (req, res) => {
  const { cedula } = req.params;

  if(!cedula){
    res.status(404).send('Cedula requerida');
    return false;
  }

  try {
    let valid = false;
    let db = 1;
    let beneficiario= '';
    const CURRENT_YEAR = new Date().getFullYear();
    const sqlQuery = `SELECT cedula_identidad, primer_nombre || ' ' || segundo_nombre || ' ' || primer_apellido || ' ' || segundo_apellido as nombre, 
    f.deno_cod_secretaria, f.deno_cod_direccion, f.demonimacion_puesto, f.cod_dep, f.cod_ficha, f.fecha_nacimiento, f.sexo, f.grupo_sanguineo, 
    f.fecha_ingreso, f.direccion_habitacion, f.telefonos_habitacion, f.carnet FROM v_cnmd06_fichas_2 as f inner join cnmd05 as t on f.cod_ficha=t.cod_ficha and f.cod_cargo=t.cod_cargo inner join cnmd01 as hn on hn.cod_dep=t.cod_dep and hn.cod_tipo_nomina=t.cod_tipo_nomina where f.cedula_identidad=${cedula} and t.ano=${CURRENT_YEAR} and f.condicion_actividad_ficha=1 [condition_ext]`;
    
    const query = await identifiedQuery({sqlQuery, table: 'f.'});
    const checkQuery = Object.values(query).reduce((acc, current) => acc+current.length,0)
    
    if(checkQuery>0){
      valid = true;
      if(query.result_db1.length>0){
        beneficiario=query.result_db1[0];
        db=1
      }else if(query.result_db2.length>0){
        beneficiario=query.result_db2[0];
        db=2
      }else if(query.result_db3.length>0){
        beneficiario=query.result_db3[0];
        db=3
      }else if(query.result_db4.length>0){
        beneficiario=query.result_db4[0];
        db=4
      }      
    }

    if(valid){ 
                 
      const result_employee = {
          ...beneficiario,
          edad: diffYear(beneficiario.fecha_nacimiento),
          antiguedad: diffYear(beneficiario.fecha_ingreso)
      };
      res.json(result_employee);
      return true;
    }else{
      res.status(404).send({message: 'No existe resultados', error: ''})
      return false;
    }

  } catch (error) {
    res.status(500).send({message: 'Error en la consulta unificada', error: error.message});
  }
})

router.get('/hoja_vida/consulta/:cedula?', async (req, res) => {
  const { cedula } = req.params;

  if(!cedula){
    res.status(404).send('Cedula requerida');
    return false;
  }

  try {
    let valid = false;
    let db = 1;
    let beneficiario= '';
    const CURRENT_YEAR = new Date().getFullYear();
    const sqlQuery = `SELECT cedula_identidad, primer_nombre || ' ' || segundo_nombre || ' ' || primer_apellido || ' ' || segundo_apellido as nombre, 
    f.deno_cod_secretaria, f.deno_cod_direccion, f.demonimacion_puesto, f.cod_dep, f.cod_ficha, f.fecha_nacimiento, f.sexo, f.grupo_sanguineo, 
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
    (select denominacion from cnmd06_profesiones where cod_profesion=rt.cod_profesion) as profesion,
    (select denominacion from cnmd06_especialidades where cod_profesion=rt.cod_profesion and cod_especialidad=rt.cod_especialidad) as especialidad,
    f.fecha_ingreso, f.direccion_habitacion, f.telefonos_habitacion, f.carnet 
    FROM v_cnmd06_fichas_2 as f 
    inner join cnmd05 as t on f.cod_ficha=t.cod_ficha and f.cod_cargo=t.cod_cargo 
    inner join cnmd01 as hn on hn.cod_dep=t.cod_dep and hn.cod_tipo_nomina=t.cod_tipo_nomina 
    inner join cnmd06_datos_registro_titulo as rt on rt.cedula=f.cedula_identidad 
    where f.cedula_identidad=${cedula} and t.ano=${CURRENT_YEAR} and f.condicion_actividad_ficha=1 [condition_ext]`;
    
    const query = await identifiedQuery({sqlQuery, table: 'f.'});
    const checkQuery = Object.values(query).reduce((acc, current) => acc+current.length,0)
    
    if(checkQuery>0){
      valid = true;
      if(query.result_db1.length>0){
        beneficiario=query.result_db1[0];
        db=1
      }else if(query.result_db2.length>0){
        beneficiario=query.result_db2[0];
        db=2
      }else if(query.result_db3.length>0){
        beneficiario=query.result_db3[0];
        db=3
      }else if(query.result_db4.length>0){
        beneficiario=query.result_db4[0];
        db=4
      }      
    }

    if(valid){ 
                 
      const result_employee = {
          ...beneficiario,
          edad: diffYear(beneficiario.fecha_nacimiento),
          antiguedad: diffYear(beneficiario.fecha_ingreso)
      };
      res.json(result_employee);
      return true;
    }else{
      res.status(404).send({message: 'No existe resultados', error: ''})
      return false;
    }

  } catch (error) {
    res.status(500).send({message: 'Error en la consulta unificada', error: error.message});
  }
})



export default router;