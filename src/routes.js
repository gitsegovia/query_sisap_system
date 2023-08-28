import express from "express";
import path from "path";
import unifiedQuery, { identifiedQuery, specificQuery } from './sequelizedb';

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
    
    const CURRENT_YEAR = new Date().getFullYear();
    const sqlQuery = `SELECT cedula_identidad, hn.periodo_desde FROM cnmd06_fichas as f inner join cnmd08_historia_transacciones as t on f.cod_ficha=t.cod_ficha and f.cod_cargo=t.cod_cargo inner join cnmd08_historia_nomina as hn on hn.cod_dep=t.cod_dep and hn.cod_tipo_nomina=t.cod_tipo_nomina and hn.numero_nomina=t.numero_nomina and hn.ano=t.ano where f.cedula_identidad=${cedula} and t.ano=${CURRENT_YEAR} and t.cod_transaccion=103 [condition_ext] order by hn.periodo_desde DESC LIMIT 1`; 
    
    const query = await identifiedQuery({sqlQuery, table: 't.'});
    const checkQuery = Object.values(query).reduce((acc, current) => acc+current.length,0)
    
    if(checkQuery>0){
      const current = new Date();

      if(query.result_db1.length>0){
        const date = new Date(query.result_db1[0].periodo_desde);
        const diffInTime = current.getTime() - date.getTime();
        const diffInDays = Math.ceil(diffInTime / (1000 * 3600 * 24))

        valid = diffInDays<= VALID_DAYS;

      }else if(query.result_db2.length>0){
        const date = new Date(query.result_db2[0].periodo_desde);
        const diffInTime = current.getTime() - date.getTime();
        const diffInDays = Math.ceil(diffInTime / (1000 * 3600 * 24))

        valid = diffInDays<= VALID_DAYS;
        db=2
      }else if(query.result_db3.length>0){
        const date = new Date(query.result_db3[0].periodo_desde);
        const diffInTime = current.getTime() - date.getTime();
        const diffInDays = Math.ceil(diffInTime / (1000 * 3600 * 24))

        valid = diffInDays<= VALID_DAYS;
        db=3
      }else if(query.result_db4.length>0){
        const date = new Date(query.result_db4[0].periodo_desde);
        const diffInTime = current.getTime() - date.getTime();
        const diffInDays = Math.ceil(diffInTime / (1000 * 3600 * 24))

        valid = diffInDays<= VALID_DAYS;
        db=4
      }      
    }else{
      const sqlQuery_obrero = `SELECT cedula_identidad FROM cnmd06_fichas as f inner join cnmd05 as t on f.cod_ficha=t.cod_ficha and f.cod_cargo=t.cod_cargo inner join cnmd01 as hn on hn.cod_dep=t.cod_dep and hn.cod_tipo_nomina=t.cod_tipo_nomina where f.cedula_identidad=${cedula} and t.ano=${CURRENT_YEAR} and f.condicion_actividad=1 and f.condicion_actividad=1 and hn.clasificacion_personal in (2,8,10) [condition_ext]`; 
      const query_obrero = await identifiedQuery({sqlQuery: sqlQuery_obrero, table: 't.'});
      
      const checkQuery = Object.values(query_obrero).reduce((acc, current) => acc+current.length,0)
      

      if(checkQuery>0){
        valid=true;
        if(query_obrero.result_db1.length>0){
          db=1;
        }else if(query_obrero.result_db2.length>0){
          db=2;
        }else if(query_obrero.result_db3.length>0){
          db=3;
        }else if(query_obrero.result_db4.length>0){
          db=4;
        }      
      }
    }

    if(valid){
      const sqlQuery_datos = `SELECT cedula_identidad, nacionalidad, primer_apellido, segundo_apellido, primer_nombre, segundo_nombre, fecha_nacimiento, sexo, direccion_habitacion, telefonos_habitacion FROM cnmd06_datos_personales WHERE cedula_identidad=${cedula}`;
      const query_datos = await specificQuery({sqlQuery: sqlQuery_datos, db});
      console.log(query_datos);
      const sqlQuery_hijos = `SELECT * FROM cnmd06_datos_hijos WHERE cedula=${cedula}`
      const query_hijos = await specificQuery({sqlQuery: sqlQuery_hijos, db});
      console.log(query_hijos);
      
      const sqlQuery_familiares = `SELECT * FROM cnmd06_datos_familiares WHERE cedula=${cedula}`
      const query_familiares = await specificQuery({sqlQuery: sqlQuery_familiares, db});
      console.log(query_familiares);
      
      const result_employee = {
        ...query_datos[0],
        afiliados: [...query_hijos,...query_familiares]

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