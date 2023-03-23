/* = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =\
||   This script for customer center (Time)                     ||
||                                                              ||
||  File Name: LMRY_CO_ART1_MPRD_v2.0.js                        ||
||                                                              ||
||  Version Date         Author        Remarks                  ||
||  2.0     Sep 04 2020  LatamReady    Use Script 2.0           ||
\= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = */
/**
 * @NApiVersion 2.0
 * @NScriptType MapReduceScript
 * @NModuleScope Public
 */
define(['N/search', 'N/config', 'N/log', 'N/file', 'N/runtime', "N/record", "./CO_Library_Mensual/LMRY_CO_Reportes_LBRY_V2.0.js",
  "/SuiteBundles/Bundle 37714/Latam_Library/LMRY_LibraryReport_LBRY_V2.js"],

  function (search, config, log, fileModulo, runtime, recordModulo, libreria, libreriaFeature) {

    var objContext = runtime.getCurrentScript();

    var LMRY_script = "LMRY_CO_ART1_MPRD_v2.0.js";

    // Parámetros
    var param_RecorID = null;
    var param_Periodo = null;
    var param_Anual = null;
    var param_Multi = null;
    var param_Feature = null;
    var param_Subsi = null;
    var param_Header = null;

    // Features
    var feature_Subsi = null;
    var feature_Multi = null;
    var hasJobsFeature = null;
    var hasAdvancedJobsFeature = null;
    var featureSpecialPeriod = null;
    var isMultiCalendar = null;

    // Fórmula de Periodos
    var formulPeriodFilters = null;

    // Period Data
    var periodenddate = null;
    var periodname = null;
    var periodstartdate = null;

    //Datos de Subsidiaria
    var companyname = null;
    var companyruc = null;

    //Language
    var language = runtime.getCurrentScript().getParameter({
      name: 'LANGUAGE'
    }).substring(0, 2);

    if (language != "en" && language != "es" && language != "pt") {
      language = "es";
    }

    function getInputData() {
      try {
        getParameterAndFeatures();
        var arrTransactions = getTransactions();

        if (arrTransactions.length != 0) {
          return arrTransactions;
        } else {
          NoData();
          return null;
        }

      } catch (err) {
        log.error('err', err);
        libreria.sendMail(LMRY_script, ' [ getInputData ] ' + err);
      }
    }


    function map(context) {
      try {
        getParameterAndFeatures();
        var key = context.key;

        var arrTemp = JSON.parse(context.value);

        //Latam - Type Concept
        var campo0 = (arrTemp[0]).substring(0, 2);
        //Campos del Customer
        var id_item = search.lookupFields({
          type: search.Type.CUSTOMER,
          id: arrTemp[1],
          columns: ['custentity_lmry_sv_taxpayer_type.custrecord_numberei', 'custentity_lmry_affectation_type']
        });

        var campo1 = id_item['custentity_lmry_sv_taxpayer_type.custrecord_numberei'];
        var campo2 = id_item.custentity_lmry_affectation_type;
        if (campo2 != '') {
          campo2 = '1';
        } else {
          campo2 = '0';
        }

        var strCreditos = campo0 + '|' + campo1 + '|' + arrTemp[2] + '|' + campo2 + ';';
        log.debug('strCreditos', strCreditos);
        context.write({
          key: key,
          value: {
            strCreditos: strCreditos
          }
        });
      } catch (err) {
        log.error('err', err);
        libreria.sendMail(LMRY_script, ' [ Map ] ' + err);
      }
    }


    function reduce(context) {

    }


    function summarize(context) {
      try {
        getParameterAndFeatures();

        var YaHuboArchivosGenerados = false;
        var text = '';
        var FilasRecorridas = 0;
        var rrr = true;
        var cont = 1;
        var text2 = '';

        context.output.iterator().each(function (key, value) {

          var obj = JSON.parse(value);

          text += (obj.strCreditos);

          FilasRecorridas++;

          var peso = lengthInUtf8Bytes(text);

          if (FilasRecorridas == 10000) {
            text2 = agruparFacturas(text);
            log.debug('text2', text2);
            SaveFile(text2, rrr, cont);
            FilasRecorridas = 0;
            YaHuboArchivosGenerados = true;
            text = '';
            rrr = false;
            cont = cont + 1;
          }
          return true;
        });

        if (FilasRecorridas != 0) {
          text2 = agruparFacturas(text);
          log.debug('text2 - !=', text2);
          if (text2 != '') {
            SaveFile(text2, rrr, cont);
          } else {
            NoData();
          }
        } else if (!YaHuboArchivosGenerados) {
          NoData();
        }
      } catch (err) {
        log.error('err', err);
        libreria.sendMail(LMRY_script, ' [ Summarize ] ' + err);
      }
    }

    function lengthInUtf8Bytes(str) {
      var m = encodeURIComponent(str).match(/%[89ABab]/g);
      return str.length + (m ? m.length : 0);
    }

    function agruparFacturas(text) {
      var a = 0;
      var arrCampos = '';
      var auxiliar = text.split(';');
      var arrayAux = new Array();
      var text2 = '';

      for (var i = 0; i < auxiliar.length - 1; i++) {
        var arrCampos = auxiliar[i].split('|');
        /*
        var campo0  = arrCampos[0];
        var campo1  = arrCampos[1];
        var campo2  = arrCampos[2];
        */
        if ((arrCampos[1] == '48' || arrCampos[1] == '2') && arrCampos[3] == '1') {
          var aux = new Array();
          aux[0] = arrCampos[0];
          aux[1] = arrCampos[2];

          arrayAux[a] = aux;
          a++;

        }
      }
      log.debug('arrayAux', arrayAux);

      //**************PERIODO********************
      var col1 = periodname;

      //**************SUBSIDIARIA********************
      if (feature_Subsi) {
        var subsi_temp = search.lookupFields({
          type: search.Type.SUBSIDIARY,
          id: param_Subsi,
          columns: ['custrecord_lmry_co_uvt_iva']
        });

        var uvt_IVA = subsi_temp.custrecord_lmry_co_uvt_iva;
      } else {

        var configpage = config.load({
          type: config.Type.COMPANY_INFORMATION
        });

        var uvt_IVA = configpage.getFieldValue('custrecord_lmry_co_uvt_iva');

      }

      for (var j = 0; j < arrayAux.length; j++) {
        var col2 = arrayAux[j][0];
        var col3 = arrayAux[j][1];

        if (j != arrayAux.length - 1) {
          while (arrayAux[j][0] == arrayAux[j + 1][0]) {
            col3 = Number(col3) + Number(arrayAux[j + 1][1]);
            j++;

            if (j == arrayAux.length - 1) {
              break;
            }
          }
        }

        if (Number(uvt_IVA) < Number(col3)) {

          if (param_Header == 'T' || param_Header == true) {
            text2 += col1 + ',' + col2 + ',' + Number(col3).toFixed(0) + '\r\n';
          } else {
            text2 += col1 + ';' + col2 + ';' + Number(col3).toFixed(0) + '\r\n';
          }
        }

      }

      log.debug('text2', text2);
      return text2;
    }

    function NoData() {

      var usuario = runtime.getCurrentUser();

      var employee = search.lookupFields({
        type: search.Type.EMPLOYEE,
        id: usuario.id,
        columns: ['firstname', 'lastname']
      });
      var usuarioName = employee.firstname + ' ' + employee.lastname;

      var report = search.lookupFields({
        type: 'customrecord_lmry_co_features',
        id: param_Feature,
        columns: ['name']
      });
      namereport = report.name;

      var generatorLog = recordModulo.load({
        type: 'customrecord_lmry_co_rpt_generator_log',
        id: param_RecorID
      });

      //Periodo
      generatorLog.setValue({
        fieldId: 'custrecord_lmry_co_rg_postingperiod',
        value: periodname
      });

      //Nombre de Archivo
      generatorLog.setValue({
        fieldId: 'custrecord_lmry_co_rg_name',
        value: 'No existe informacion para los criterios seleccionados.'
      });
      //Creado Por
      generatorLog.setValue({
        fieldId: 'custrecord_lmry_co_rg_employee',
        value: usuarioName
      });
      generatorLog.setValue({
        fieldId: 'custrecord_lmry_co_rg_transaction',
        value: namereport
      });

      var recordId = generatorLog.save();
    }

    function ObtainNameSubsidiaria(subsidiary) {
      try {
        if (subsidiary != '' && subsidiary != null) {
          var subsidyName = search.lookupFields({
            type: search.Type.SUBSIDIARY,
            id: subsidiary,
            columns: ['legalname']
          });

          return subsidyName.legalname
        }
      } catch (err) {
        libreria.sendMail(LMRY_script, ' [ ObtainNameSubsidiaria ] ' + err);
      }
      return '';
    }

    function ObtainFederalIdSubsidiaria(subsidiary) {
      try {
        if (subsidiary != '' && subsidiary != null) {
          var federalId = search.lookupFields({
            type: search.Type.SUBSIDIARY,
            id: subsidiary,
            columns: ['taxidnum']
          });

          return federalId.taxidnum
        }
      } catch (err) {
        libreria.sendMail(LMRY_script, ' [ ObtainFederalIdSubsidiaria ] ' + err);
      }
      return '';
    }


    function ObtenerDatosSubsidiaria() {
      var configpage = config.load({
        type: config.Type.COMPANY_INFORMATION
      });

      if (feature_Subsi) {
        companyname = ObtainNameSubsidiaria(param_Subsi);
        companyruc = ObtainFederalIdSubsidiaria(param_Subsi);
      } else {
        companyruc = configpage.getFieldValue('employerid');
        companyname = configpage.getFieldValue('legalname');
      }

      companyruc = companyruc.replace(' ', '');
    }

    function SaveFile(strAuxiliar, rrr, cont) {

      var objContext = runtime.getCurrentScript();
      var arreglo = new Array();
      var arregloSub = new Array();
      ObtenerDatosSubsidiaria();

      try {
        var folderId = objContext.getParameter({
          name: 'custscript_lmry_file_cabinet_rg_co'
        });

        var report = search.lookupFields({
          type: 'customrecord_lmry_co_features',
          id: param_Feature,
          columns: ['name']
        });
        namereport = report.name;

        var AAAA = periodname;

        if (feature_Multi) {
          var multibookName_temp = search.lookupFields({
            type: search.Type.ACCOUNTING_BOOK,
            id: param_Multi,
            columns: ['name']
          });

          var multibookName = multibookName_temp.name;
        }

        if (param_Header == 'T' || param_Header == true) {

          if (feature_Multi) {
            if (cont == 1) {
              var fileName = 'ART1' + '_' + companyruc + '_' + AAAA + '_' + param_Subsi + '_' + param_Multi + '.csv';
            } else {
              var fileName = 'ART1' + '_' + companyruc + '_' + AAAA + '_' + param_Subsi + '_' + param_Multi + '_' + cont + '.csv';
            }
          } else {
            if (cont == 1) {
              var fileName = 'ART1' + '_' + companyruc + '_' + AAAA + '_' + param_Subsi + '.csv';
            } else {
              var fileName = 'ART1' + '_' + companyruc + '_' + AAAA + '_' + param_Subsi + '_' + cont + '.csv';
            }
          }

        } else {

          if (feature_Multi) {
            if (cont == 1) {
              var fileName = 'ART1' + '_' + companyruc + '_' + AAAA + '_' + param_Subsi + '_' + param_Multi + '.txt';
            } else {
              var fileName = 'ART1' + '_' + companyruc + '_' + AAAA + '_' + param_Subsi + '_' + param_Multi + '_' + cont + '.txt';
            }
          } else {
            if (cont == 1) {
              var fileName = 'ART1' + '_' + companyruc + '_' + AAAA + '_' + param_Subsi + '.txt';
            } else {
              var fileName = 'ART1' + '_' + companyruc + '_' + AAAA + '_' + param_Subsi + '_' + cont + '.txt';
            }
          }

        }



        // Almacena en la carpeta de Archivos Generados
        if (folderId != '' && folderId != null) {
          // Extension del archivo
          // Crea el archivo
          if (param_Header == 'T' || param_Header == true) {

            var globalLabels = getGlobalLabels();
            var titulo = globalLabels.cabecera[language];

            strAuxiliar = titulo + strAuxiliar;
            log.debug('strAuxiliar', strAuxiliar);

            var percepcionFile = fileModulo.create({
              name: fileName,
              fileType: fileModulo.Type.CSV,
              contents: strAuxiliar,
              encoding: fileModulo.Encoding.UTF8,
              folder: folderId
            });
          } else {
            var percepcionFile = fileModulo.create({
              name: fileName,
              fileType: fileModulo.Type.PLAINTEXT,
              contents: strAuxiliar,
              encoding: fileModulo.Encoding.UTF8,
              folder: folderId
            });
          }


          var idfile = percepcionFile.save(); // Termina de grabar el archivo
          param_IDFiles = idfile;
          log.debug({ title: 'param_IDFiles', details: param_IDFiles });
          // Trae URL de archivo generado
          var idfile2 = fileModulo.load({
            id: idfile
          });

          // Obtenemos de las prefencias generales el URL de Netsuite (Produccion o Sandbox)
          var getURL = objContext.getParameter({
            name: 'custscript_lmry_netsuite_location'
          });

          var urlfile = '';

          if (getURL != '' && getURL != '') {
            urlfile += 'https://' + getURL;
          }
          urlfile += idfile2.url;

          if (idfile) {
            var usuario = runtime.getCurrentUser();
            var employee = search.lookupFields({
              type: search.Type.EMPLOYEE,
              id: usuario.id,
              columns: ['firstname', 'lastname']
            });
            var usuarioName = employee.firstname + ' ' + employee.lastname;

            if (cont > 1) {
              var recordLog = recordModulo.create({
                type: 'customrecord_lmry_co_rpt_generator_log'
              });

              //Nombre de Archivo
              recordLog.setValue({
                fieldId: 'custrecord_lmry_co_rg_name',
                value: fileName
              });

              //Url de Archivo
              recordLog.setValue({
                fieldId: 'custrecord_lmry_co_rg_url_file',
                value: urlfile
              });

              //Nombre de Reporte
              recordLog.setValue({
                fieldId: 'custrecord_lmry_co_rg_transaction',
                value: reportName
              });

              //Nombre de Subsidiaria
              recordLog.setValue({
                fieldId: 'custrecord_lmry_co_rg_subsidiary',
                value: companyname
              });

              //Periodo
              recordLog.setValue({
                fieldId: 'custrecord_lmry_co_rg_postingperiod',
                value: periodname
              });

              //Multibook
              recordLog.setValue({
                fieldId: 'custrecord_lmry_co_rg_multibook',
                value: multibookName
              });

              //Creado Por
              recordLog.setValue({
                fieldId: 'custrecord_lmry_co_rg_employee',
                value: usuarioName
              });

              recordLog.save();
            } else {
              var recordLog = recordModulo.load({
                type: 'customrecord_lmry_co_rpt_generator_log',
                id: param_RecorID
              });

              //Periodo
              recordLog.setValue({
                fieldId: 'custrecord_lmry_co_rg_postingperiod',
                value: periodname
              });

              //Nombre de Archivo
              recordLog.setValue({
                fieldId: 'custrecord_lmry_co_rg_name',
                value: fileName
              });

              //Url de Archivo
              recordLog.setValue({
                fieldId: 'custrecord_lmry_co_rg_url_file',
                value: urlfile
              });

              //Creado Por
              recordLog.setValue({
                fieldId: 'custrecord_lmry_co_rg_employee',
                value: usuarioName
              });

              recordLog.save();

            }

            libreriaFeature.sendConfirmUserEmail(namereport, 3, fileName, language);
          }

        } else {
          // Debug
          log.error({
            title: 'Creacion de File:',
            details: 'No existe el folder'
          });
        }

      } catch (err) {
        log.error('ERROR 2', err);
      }
    }

    function getParameterAndFeatures() {
      var objContext = runtime.getCurrentScript();

      // Parámetros
      param_RecorID = objContext.getParameter({ name: 'custscript_lmry_co_art1_recordid' });
      param_Periodo = objContext.getParameter({ name: 'custscript_lmry_co_art1_periodo' });
      param_Anual = objContext.getParameter({ name: 'custscript_lmry_co_art1_periodo_anual' });
      param_Multi = objContext.getParameter({ name: 'custscript_lmry_co_art1_multibook' });
      param_Feature = objContext.getParameter({ name: 'custscript_lmry_co_art1_feature' });
      param_Subsi = objContext.getParameter({ name: 'custscript_lmry_co_art1_subsidiaria' });
      param_Header = objContext.getParameter({ name: 'custscript_lmry_co_art1_cabecera' });

      // Features
      feature_Subsi = runtime.isFeatureInEffect({ feature: "SUBSIDIARIES" });
      feature_Multi = runtime.isFeatureInEffect({ feature: "MULTIBOOK" });
      hasJobsFeature = runtime.isFeatureInEffect({ feature: 'JOBS' });
      hasAdvancedJobsFeature = runtime.isFeatureInEffect({ feature: 'ADVANCEDJOBS' });
      isMultiCalendar = runtime.isFeatureInEffect({ feature: 'MULTIPLECALENDARS' });

      log.debug('Parámetros', param_RecorID + '---' + param_Periodo + '---' + param_Anual + '---' + param_Multi + '---' + param_Feature + '---' + param_Subsi + '---' + param_Header);
      log.debug('Features Jobs', hasJobsFeature + '---' + hasAdvancedJobsFeature);

      /** DESVINCULACION */
      featureSpecialPeriod = getFeatures(677);
      log.debug('Hay Special', featureSpecialPeriod);
      if (featureSpecialPeriod == true || featureSpecialPeriod == 'T') {
        //Period Name
        periodname = param_Anual;

        //Period Start Date
        periodstartdate = '01/01/' + param_Anual;
        //Period End Date
        periodenddate = '31/12/' + param_Anual;

        //Obteniendo los IDs de los periodos del record Special Accounting Periods
        var specialPeriodsIDs = getSpecialPeriods(param_Anual);
        if (specialPeriodsIDs.length != 0) {
          //Armando la fórmula de periodos
          formulPeriodFilters = generarStringFilterPostingPeriodAnual(specialPeriodsIDs);
        }
      } else {
        //Period name, start date y enddate
        var periodenddate_temp = search.lookupFields({
          type: search.Type.ACCOUNTING_PERIOD,
          id: param_Anual,
          columns: ['enddate', 'periodname', 'startdate']
        });

        periodenddate = periodenddate_temp.enddate;
        periodstartdate = periodenddate_temp.startdate;
        //Period Name
        periodname = periodenddate_temp.periodname;
        var yearPeriod = periodname.split(' ');
        periodname = yearPeriod[1];

        // Obtener Filtro de fecha
        var arregloidPeriod = getPeriods(periodstartdate, periodenddate);
        if (arregloidPeriod.length != 0) {
          //Armando la fórmula de periodos
          formulPeriodFilters = generarStringFilterPostingPeriodAnual(arregloidPeriod);
        }
      }

    }

    function getTransactions() {
      var intDMinReg = 0;
      var intDMaxReg = 1000;
      var DbolStop = false;
      var ArrReturn = [];

      var savedsearch = search.create({
        type: "invoice",
        filters:
          [
            ["type", "anyof", "CustInvc"],
            "AND",
            ["posting", "is", "T"],
            "AND",
            ["memorized", "is", "F"],
            "AND",
            ["voided", "is", "F"],
            "AND",
            ["mainline", "is", "F"],
            "AND",
            ["custbody_lmry_type_concept", "noneof", "@NONE@"],
            "AND",
            ["taxline", "is", "F"],
            "AND",
            ["shipping", "is", "F"],
            "AND",
            ["cogs", "is", "F"],
            "AND",
            ["formulatext: CASE WHEN {taxitem} IN ('UNDEF-CO', 'UNDEF_CO') THEN 1 ELSE 0 END", "is", "0"]
          ],
        columns:
          [
            search.createColumn({
              name: "custbody_lmry_type_concept",
              summary: "GROUP",
              sort: search.Sort.ASC,
              label: "Latam - Type Concept"
            }),
            search.createColumn({
              name: "formulacurrency",
              summary: "SUM",
              formula: "{amount}",
              label: "Formula (Currency)"
            })
          ],
        settings: []
      });

      if (hasJobsFeature && !hasAdvancedJobsFeature) {
        log.debug("customermain");
        var customerColumn = search.createColumn({
          name: 'formulanumeric',
          formula: '{customermain.internalid}',
          summary: 'GROUP'
        });
        savedsearch.columns.push(customerColumn);
      } else if ((!hasJobsFeature && !hasAdvancedJobsFeature) || (!hasJobsFeature && hasAdvancedJobsFeature) || (hasJobsFeature && hasAdvancedJobsFeature)) {
        log.debug("customer");
        var customerColumn = search.createColumn({
          name: "formulanumeric",
          formula: "CASE WHEN NVL({job.internalid},-1) = -1 THEN {customer.internalid} ELSE {job.customer.id} end",
          summary: "GROUP"
        });
        savedsearch.columns.push(customerColumn);
      }

      if (feature_Subsi) {
        var subsidiaryFilter = search.createFilter({
          name: 'subsidiary',
          operator: search.Operator.IS,
          values: [param_Subsi]
        });
        savedsearch.filters.push(subsidiaryFilter);

        var confiConsolidationType = search.createSetting({
          name: 'consolidationtype',
          value: 'NONE'
        });
        savedsearch.settings.push(confiConsolidationType);
      }

      // Filtro Fórmula (Text) con los IDs de los periodos
      log.debug('formulPeriodFilters', formulPeriodFilters);
      var periodFilter = search.createFilter({
        name: "formulatext",
        formula: formulPeriodFilters,
        operator: search.Operator.IS,
        values: "1"
      });
      savedsearch.filters.push(periodFilter);

      if (feature_Multi) {
        var multibookFilter = search.createFilter({
          name: 'accountingbook',
          join: 'accountingtransaction',
          operator: search.Operator.IS,
          values: [param_Multi]
        });
        savedsearch.filters.push(multibookFilter);

        var exchangerateColum = search.createColumn({
          name: 'formulacurrency',
          summary: "sum",
          formula: "{accountingtransaction.amount}"
        });
        savedsearch.columns.push(exchangerateColum);
      }

      var searchResult = savedsearch.run();

      var auxiliar = '';
      while (!DbolStop) {
        var objResult = searchResult.getRange(intDMinReg, intDMaxReg);
        if (objResult != null) {
          if (objResult.length != 1000) {
            DbolStop = true;
          }
          var contador = 1;
          for (var i = 0; i < objResult.length; i++) {
            var columns = objResult[i].columns;
            var arrAuxiliar = new Array();
            //0. Type concept
            if (objResult[i].getValue(columns[0]) != '' && objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '-  None -') {
              arrAuxiliar[0] = objResult[i].getText(columns[0]);
            } else {
              arrAuxiliar[0] = '';
            }
            //1. Customer InternalID
            if (objResult[i].getValue(columns[2]) != '' && objResult[i].getValue(columns[2]) != null && objResult[i].getValue(columns[2]) != '-  None -') {
              arrAuxiliar[1] = objResult[i].getValue(columns[2]);
            } else {
              arrAuxiliar[1] = '';
            }
            //2. Amount
            if (feature_Multi) {
              if (objResult[i].getValue(columns[3]) != '' && objResult[i].getValue(columns[3]) != null && objResult[i].getValue(columns[3]) != '-  None -') {
                arrAuxiliar[2] = objResult[i].getValue(columns[3]);
              } else {
                arrAuxiliar[2] = '';
              }
            } else {
              if (objResult[i].getValue(columns[1]) != '' && objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '-  None -') {
                arrAuxiliar[2] = objResult[i].getValue(columns[1]);
              } else {
                arrAuxiliar[2] = '';
              }
            }

            ArrReturn.push(arrAuxiliar);
          }
          if (!DbolStop) {
            intDMinReg = intDMaxReg;
            intDMaxReg += 1000;
          }
        } else {
          DbolStop = true;
        }
      }

      log.debug('ArrReturn', ArrReturn);
      return ArrReturn;
    }

    function getFeatures(idFeature) {
      var isActivate = false;
      var licenses = new Array();

      licenses = libreriaFeature.getLicenses(param_Subsi);
      isActivate = libreriaFeature.getAuthorization(idFeature, licenses);

      return isActivate;
    }

    function getPeriods(startDateAux, endDateAux) {
      var period = new Array();
      var varFilter = new Array();
      if (isMultiCalendar) {
        var varSubsidiary = search.lookupFields({
          type: 'subsidiary',
          id: param_Subsi,
          columns: ['fiscalcalendar']
        });
        var fiscalCalendar = varSubsidiary.fiscalcalendar[0].value;
        var accountingperiodObj = search.create({
          type: 'accountingperiod',
          filters: [
            ['isyear', 'is', 'F'],
            'AND',
            ['isquarter', 'is', 'F'],
            'AND',
            ['isadjust', 'is', 'F'],
            'AND',
            ['fiscalcalendar', 'anyof', fiscalCalendar],
            'AND',
            ['startdate', 'onorafter', startDateAux],
            'AND',
            ['enddate', 'onorbefore', endDateAux]
          ],
          columns: [
            search.createColumn({
              name: "periodname",
              summary: "GROUP",
              label: "Name"
            }),
            search.createColumn({
              name: "startdate",
              summary: "GROUP",
              sort: search.Sort.ASC,
              label: "Start Date"
            }),
            search.createColumn({
              name: "enddate",
              summary: "GROUP",
              label: "End Date"
            }),
            search.createColumn({
              name: "internalid",
              summary: "GROUP",
              label: "Internal ID"
            })
          ]
        });
      } else {
        var accountingperiodObj = search.create({
          type: 'accountingperiod',
          filters: [
            ['isyear', 'is', 'F'],
            'AND',
            ['isquarter', 'is', 'F'],
            'AND',
            ['isadjust', 'is', 'F'],
            'AND',
            ['startdate', 'onorafter', startDateAux],
            'AND',
            ['enddate', 'onorbefore', endDateAux]
          ],
          columns: [
            search.createColumn({
              name: "periodname",
              summary: "GROUP",
              label: "Name"
            }),
            search.createColumn({
              name: "startdate",
              summary: "GROUP",
              sort: search.Sort.ASC,
              label: "Start Date"
            }),
            search.createColumn({
              name: "enddate",
              summary: "GROUP",
              label: "End Date"
            }),
            search.createColumn({
              name: "internalid",
              summary: "GROUP",
              label: "Internal ID"
            })
          ]
        });
      }

      // Ejecutando la busqueda
      var varResult = accountingperiodObj.run();
      var AccountingPeriodRpt = varResult.getRange({
        start: 0,
        end: 1000
      });
      if (AccountingPeriodRpt == null || AccountingPeriodRpt.length == 0) {
        log.debug('NO DATA', 'No hay periodos para ese año seleccionado');
        return false;
      } else {
        var columns;
        for (var i = 0; i < AccountingPeriodRpt.length; i++) {
          columns = AccountingPeriodRpt[i].columns;
          period[i] = new Array();
          period[i] = AccountingPeriodRpt[i].getValue(columns[3]);
        }
      }

      return period;
    }

    function getSpecialPeriods(year) {
      var specialPeriods_ID = [];
      var searchPeriodSpecial = search.create({
        type: "customrecord_lmry_special_accountperiod",
        filters: [
          ["isinactive", "is", "F"],
          "AND",
          ["custrecord_lmry_anio_fisco", "is", year],
          "AND",
          ["custrecord_lmry_adjustment", "is", "F"]
        ],
        columns: [
          search.createColumn({
            name: "custrecord_lmry_accounting_period"
          })
        ]
      });

      if (isMultiCalendar == true || isMultiCalendar == 'T') {

        var subsiCalendar = search.lookupFields({
          type: search.Type.SUBSIDIARY,
          id: param_Subsi,
          columns: ['fiscalcalendar']
        });

        calendarSub = {
          id: subsiCalendar.fiscalcalendar[0].value,
          nombre: subsiCalendar.fiscalcalendar[0].text
        }
        calendarSub = JSON.stringify(calendarSub);

        var fiscalCalendarFilter = search.createFilter({
          name: 'custrecord_lmry_calendar',
          operator: search.Operator.IS,
          values: calendarSub
        });
        searchPeriodSpecial.filters.push(fiscalCalendarFilter);
      }

      var searchResult = searchPeriodSpecial.run().getRange(0, 100);

      if (searchResult.length != 0) {
        for (i = 0; i < searchResult.length; i++) {
          var columns = searchResult[i].columns;
          specialPeriods_ID.push(searchResult[i].getValue(columns[0]));
        }
      } else {
        log.error('Alerta', 'No existe periodos configurados en el record Special Periods, se tomaron los periodos normales del accounting period de Netsuite que esten entre ' +
          'el 1 de Enero y el 31 Diciembre del año seleccionado.');

        var arregloidPeriod = getPeriods(periodstartdate, periodenddate);
        formulPeriodFilters = generarStringFilterPostingPeriodAnual(arregloidPeriod);
      }

      return specialPeriods_ID;
    }

    function generarStringFilterPostingPeriodAnual(idsPeriod) {
      var cant = idsPeriod.length;
      var comSimpl = "'";
      var strinic = "CASE WHEN ({postingperiod.id}=" + comSimpl + idsPeriod[0] + comSimpl;
      var strAdicionales = "";
      var strfinal = ") THEN 1 ELSE 0 END";
      for (var i = 1; i < cant; i++) {
        strAdicionales += " or {postingperiod.id}=" + comSimpl + idsPeriod[i] + comSimpl;
      }
      var str = strinic + strAdicionales + strfinal;
      return str;
    }

    function getGlobalLabels() {
      var labels = {
        cabecera: {
          en: 'VALIDITY' + ',' + 'INCOME CONCEPTS' + ',' + 'VALUE OF ACTIVITIES NOT SUBJECT' + '\r\n',
          es: 'VIGENCIA' + ',' + 'CONCEPTOS DE INGRESOS' + ',' + 'VALOR DE LAS ACTIVIDADES NO SUJETAS' + '\r\n',
          pt: 'VALIDADE' + ',' + 'CONCEITOS DE RENDA' + ',' + 'VALOR DAS ATIVIDADES NAO SUJEITOS' + '\r\n'
        }
      }

      return labels;
    }

    return {
      getInputData: getInputData,
      map: map,
      summarize: summarize
    };

  });