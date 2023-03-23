/* = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =\
||   This script for customer center (Time)                     ||
||                                                              ||
||  File Name: LMRY_CO_BalCompTerceros_MPRD_v2.0.js             ||
||                                                              ||
||  Version Date         Author        Remarks                  ||
||  2.0     Jun 18 2018  LatamReady    Use Script 2.0           ||
\= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = */
/**
 * @NApiVersion 2.0
 * @NScriptType MapReduceScript
 * @NModuleScope Public
 */
define(['N/search', 'N/log', 'require', 'N/file', 'N/runtime', 'N/query', "N/format", "N/record", "N/task", "N/config", "/SuiteBundles/Bundle 37714/Latam_Library/LMRY_LibraryReport_LBRY_V2.js"],

  function(search, log, require, fileModulo, runtime, query, format, recordModulo, task, config, libRpt) {
    /**
     * Input Data for processing
     *
     * @return Array,Object,Search,File
     *
     * @since 2016.1
     */
    var objContext = runtime.getCurrentScript();
    var LMRY_script = "LMRY_CO_BalCompTerceros_MPRD_v2.0.js";

    var paramEntity = objContext.getParameter({
      name: 'custscript_lmry_terceros_mprdc_entity'
    });
    var paramMultibook = objContext.getParameter({
      name: 'custscript_lmry_terceros_mprdc_multi'
    });
    var paramRecordID = objContext.getParameter({
      name: 'custscript_lmry_terceros_mprdc_record'
    });
    var paramSubsidy = objContext.getParameter({
      name: 'custscript_lmry_terceros_mprdc_subsi'
    });
    var paramPeriod = objContext.getParameter({
      name: 'custscript_lmry_terceros_mprdc_period'
    });
    var paramPUC = objContext.getParameter({
      name: 'custscript_lmry_terceros_mprdc_lastpuc'
    });
    var paramPeriodFin = objContext.getParameter({
      name: 'custscript_lmry_terceros_mprdc_periodFin'
    });
    var paramAdjustment = objContext.getParameter({
      name: 'custscript_lmry_terceros_mprdc_adjust'
    });
    var paramOpenBalance = objContext.getParameter({
      name: 'custscript_lmry_terceros_mprdc_opbalance'
    });
    var paramPuc8D = objContext.getParameter({
      name: 'custscript_lmry_terceros_mprdc_8d'
    });

    var periodYearIni;
    var periodMonthIni;

    var periodstartdateIni;
    var periodenddateIni;
    var periodenddateFin;

    var calendarSubsi;
    var featAccountingSpecial;

    var featuresubs = runtime.isFeatureInEffect({
      feature: "SUBSIDIARIES"
    });
    var feamultibook = runtime.isFeatureInEffect({
      feature: "MULTIBOOK"
    });
    var featurePeriodEnd = runtime.isFeatureInEffect({
      feature: "PERIODENDJOURNALENTRIES"
    });
    var featureCalendars = runtime.isFeatureInEffect({
      feature: "MULTIPLECALENDARS"
    });
    var language = runtime.getCurrentScript().getParameter({
      name: 'LANGUAGE'
    }).substring(0, 2);

    var entity_name;
    var entity_id;
    var entity_nit;

    function getInputData() {
      try {
        ParametrosYFeatures();
        var ArrData = new Array();
        var ArrDataRestante = new Array();
        var ArrDataActual = new Array(); //Movimientos

        // Obtiene años ya procesados
        var processedYears = ObtenerAñosProcesados();
        OrdenarAños(processedYears);

        if (featAccountingSpecial || featAccountingSpecial == 'T') {

          var ArrYears = getFiscalYearsSpecialPeriod();
          log.debug('[getInputData] ArrYears getFiscalYearsSpecialPeriod', ArrYears);
          for (var i = 0; i < ArrYears.length; i++) {
            var flag = false;

            for (var j = 0; j < processedYears.length; j++) {
              if (processedYears[j][1] == ArrYears[i] && processedYears[j][4] == paramPUC) {
                flag = true;
                break;
              }
            }

            if (!flag) {
              var arrTemporal = new Array();
              var periodsYear = getSpecialPeriods(ArrYears[i]);
              log.debug('[getInputData] periodsYear getSpecialPeriods', periodsYear);
              arrTemporal = ObtenerData(periodsYear, "saldo_anterior", true);

              if (arrTemporal.length != 0) {
                arrTemporal = AgruparPorCuenta(arrTemporal);

                for (var x = 0; x < arrTemporal.length; x++) {
                  arrTemporal[x].push(ArrYears[i]); //6
                  ArrData.push(arrTemporal[x]);
                }
              }

              actualizarThirdProc(ArrYears[i]);
            }

          }

        } else {
          // Obtiene los periodos Fiscal Year
          var ArrYears = ObtenerAñosFiscales();
          OrdenarAños(ArrYears);

          for (var i = 0; i < ArrYears.length; i++) {
            var flag = false;

            for (var j = 0; j < processedYears.length; j++) {
              if (processedYears[j][1] == ArrYears[i][1] && processedYears[j][4] == paramPUC) {
                flag = true;
                break;
              }
            }

            if (!flag) {
              var arrTemporal = new Array();
              //ArrYears:  id del year
              arrTemporal = ObtenerData(ArrYears[i][0], "saldo_anterior", true);

              if (arrTemporal.length != 0) {
                arrTemporal = AgruparPorCuenta(arrTemporal);

                for (var x = 0; x < arrTemporal.length; x++) {
                  arrTemporal[x].push(ArrYears[i][1]); //6
                  ArrData.push(arrTemporal[x]);
                }
              }

              actualizarThirdProc(ArrYears[i][1]);
            }
          }

        }

        log.debug('[getInputData] ArrData', ArrData);

        var allPeriods = ObtenerPeriodos();
        log.debug('allPeriods', allPeriods);
        var periodsOfYear = ObtenerPeriodosDelAño(allPeriods);
        log.debug('periodsOfYear', periodsOfYear);
        /************************* Obtiene data de los periodos restantes **********************/
        // Obtener periodos del año
        if (periodsOfYear.length != 0) {
          //ArrYears = [[internalid, id periodo mensual]]
          ArrDataRestante = ObtenerData(periodsOfYear, "saldo_restante", false);
        }
        log.debug('ArrDataRestante getInputData', ArrDataRestante);
        /************************** Obtiene Movimientos *************************************/
        if (paramPeriodFin != null) {
          ArrDataActual = ObtenerData(paramPeriod, "movimientos", false, paramPeriodFin);
        } else {
          ArrDataActual = ObtenerData(paramPeriod, "movimientos", false);
        }
        log.debug('ArrDataActual getInputData movimientos', ArrDataActual);

        return ArrData.concat(ArrDataRestante, ArrDataActual);

      } catch (err) {
        log.error('err', err);
        libRpt.sendErrorEmail(err, LMRY_script, language);
      }
    }

    /**
     * If this entry point is used, the map function is invoked one time for each key/value.
     *
     * @param {Object} context
     * @param {boolean} context.isRestarted - Indicates whether the current invocation represents a restart
     * @param {number} context.executionNo - Version of the bundle being installed
     * @param {Iterator} context.errors - This param contains a "iterator().each(parameters)" function
     * @param {string} context.key - The key to be processed during the current invocation
     * @param {string} context.value - The value to be processed during the current invocation
     * @param {function} context.write - This data is passed to the reduce stage
     *
     * @since 2016.1
     */
    function map(context) {
      try {
        var arrTemp = JSON.parse(context.value);
        //log.debug('arrTemp map', arrTemp);
        if (paramPUC == '' || paramPUC == null) {
          paramPUC = 1;
        }
        /* VALIDACIÓN DE PUC DENTRO DEL RANGO paramPUC y detalle 8d segun el caso */
        var pucNum = getAndValidatePuc(arrTemp, paramPUC);

        if (pucNum != '') {
          /* VALIDACIÓN BALANCE TOTAL DIFERENTE DE 0 */

          var balance = Number(arrTemp[1]) - Number(arrTemp[2]);

          if (balance != 0) {
            if (arrTemp[4] == 'movimientos' || arrTemp[4] == 'saldo_restante') {
              var json_entity = {};
              var flag_entity = ObtenerEntidad(arrTemp[3]);

              if (flag_entity) {
                json_entity.name = removeSpecialCharacter(entity_name);
                json_entity.nit = entity_nit;
                json_entity.internalid = arrTemp[3];

                arrTemp[3] = JSON.stringify(json_entity);
              } else {
                arrTemp[3] = '';
              }

              var typeData = arrTemp[4];

              context.write({
                key: context.key,
                value: {
                  arreglo: arrTemp, //Vector
                  Clase: typeData
                }
              });

            } else {
              /* GUARDADO DE SALDOS EN AÑOS ANTERIORES */
              actualizarThirdData(arrTemp, pucNum);
            }

          } else {
            log.debug('[map] Alert - balance es 0', arrTemp);
          }
        }

      } catch (err) {
        log.error('err map', err);
      }
    }

    /**
     * If this entry point is used, the reduce function is invoked one time for
     * each key and list of values provided..
     *
     * @param {Object} context
     * @param {boolean} context.isRestarted - Indicates whether the current invocation of the represents a restart.
     * @param {number} context.concurrency - The maximum concurrency number when running the map/reduce script.
     * @param {Date} 0context.datecreated - The time and day when the script began running.
     * @param {number} context.seconds - The total number of seconds that elapsed during the processing of the script.
     * @param {number} context.usage - TThe total number of usage units consumed during the processing of the script.
     * @param {number} context.yields - The total number of yields that occurred during the processing of the script.
     * @param {Object} context.inputSummary - Object that contains data about the input stage.
     * @param {Object} context.mapSummary - Object that contains data about the map stage.
     * @param {Object} context.reduceSummary - Object that contains data about the reduce stage.
     * @param {Iterator} context.output - This param contains a "iterator().each(parameters)" function
     *
     * @since 2016.1
     */
    function summarize(context) {
      try {
        ParametrosYFeatures();
        var ArrData = new Array();
        /***** VALIDACION SALDO INICIAL PARA CUENTAS DE RESULTADOS ****/
        var considerPastYears = true;
        if (paramPUC == 4 || paramPUC == 5 || paramPUC == 6 || paramPUC == 7) {
          if (paramOpenBalance == 'T' || paramOpenBalance == true) {
            considerPastYears = false;
          }
        }

        if (considerPastYears) {
          /************ OBTENIENDO SALDOS DE AÑOS ANTERIORES ********/
          if (featAccountingSpecial || featAccountingSpecial == 'T') {
            var ArrYears = getFiscalYearsSpecialPeriod();
            var ArrYearsForSearch = new Array();

            for (var i = 0; i < ArrYears.length; i++) {
              if (ArrYears[i] < periodYearIni) {
                ArrYearsForSearch.push(ArrYears[i]);
              }
            }

          } else {
            var ArrYears = ObtenerAñosFiscales();
            OrdenarAños(ArrYears);
            var ArrYearsForSearch = new Array();

            for (var i = 0; i < ArrYears.length; i++) {
              if (ArrYears[i][1] < periodYearIni) {
                ArrYearsForSearch.push(ArrYears[i][1]);
              }
            }

          }
          log.debug('ArrYearsForSearch', ArrYearsForSearch);

          if (ArrYearsForSearch.length != 0) {
            ArrData = ObtenerSaldoAntRecord(ArrYearsForSearch[0], ArrYearsForSearch[ArrYearsForSearch.length - 1]);
            log.debug('ArrData ObtenerSaldoAntRecord', ArrData);
          }
        }

        var arrDataRestante = new Array();
        var arrDataMovimientos = new Array();

        context.output.iterator().each(function(key, value) {
          var obj = JSON.parse(value);
          var clase = obj.Clase;
          if (clase == 'movimientos') {
            arrDataMovimientos.push(obj.arreglo);
          } else if (clase == 'saldo_restante') {
            arrDataRestante.push(obj.arreglo);
          }
          return true;
        });
        log.debug('arrDataRestante', arrDataRestante);
        log.debug('arrDataMovimientos', arrDataMovimientos);
        //Agrupa data de los periodos restantes (que se incluyen dentro del saldo anterior)
        if (arrDataRestante.length != 0) {
          arrDataRestante = AgruparPorCuenta(arrDataRestante);
        }
        //JUNTA SALDO ANTERIOR : De años pasados y periodos restantes
        ArrData = JuntarYAgruparArreglos(ArrData, arrDataRestante);
        //Agrupa data de Movimientos
        if (arrDataMovimientos.length != 0) {
          arrDataMovimientos = AgruparPorCuenta(arrDataMovimientos);
        }

        ArrData = ObtenerArregloFinalSeisDigitos(ArrData, arrDataMovimientos);
        //log.debug('summarize ArrData',ArrData);
        var strDataTotal = ConvertirAString(ArrData);
        //log.debug('strDataTotal',strDataTotal);
        var idfile = savefile(strDataTotal);

        LanzarSchedule(idfile);

      } catch (err) {
        log.error('err', err);
        libRpt.sendErrorEmail(err, LMRY_script, language);
      }
    }

    function existPuc8d(){

      var pucSearch = search.create({
        type: "customrecord_lmry_co_puc",
        filters: [
          ["isinactive", "is", "F"], "AND",
          ["formulatext: LENGTH({name})", "is", "8"]
        ],
        columns: ["name"]
      });

      var pagedData = pucSearch.runPaged({
        pageSize: 1000
      });

      pagedData.pageRanges.forEach(function(pageRange) {
        page = pagedData.fetch({
          index: pageRange.index
        });

        page.data.forEach(function(result) {
          log.debug('result', result);
        })
      });


      var searchResultCount = pucSearch.runPaged().count;
      return searchResultCount > 0 ? true : false;
    }

    function periodsIdByDate(initialData, finalDate) {
      var periodsId = new Array();

      var periodSearch = search.create({
        type: "accountingperiod",
        filters: [
          ["isquarter", "is", "F"],
          "AND",
          ["isyear", "is", "F"],
          "AND",
          ["isinactive", "is", "F"],
          "AND",
          ["startdate", "onorafter", initialData],
          "AND",
          ["startdate", "onorbefore", finalDate]
        ],
        columns: [
          search.createColumn({
            name: "internalid",
            label: "Internal ID"
          })
        ]
      });

      periodSearch.run().each(function(result) {
        periodsId.push(result.getValue('internalid'));
        return true;
      });

      return periodsId;
    }

    function getAndValidatePuc(arrData, paramPUC) {
      var pucResult = '';

      var accountRec = search.lookupFields({
        type: search.Type.ACCOUNT,
        id: Number(arrData[0]),
        columns: ['custrecord_lmry_co_puc_d6_id', 'custrecord_lmry_co_puc_id']
      });

      var pucCod = '';

      if (arrData[4] == 'saldo_anterior') {
        if (arrData[5]) {//detalle a puc 8d?
          pucCod = accountRec.custrecord_lmry_co_puc_id;

        } else {
          pucCod = accountRec.custrecord_lmry_co_puc_d6_id;
        }
      } else {
        if (paramPuc8D == 'T') {
          pucCod = accountRec.custrecord_lmry_co_puc_id;
        } else {
          pucCod = accountRec.custrecord_lmry_co_puc_d6_id;
        }
      }


      if (pucCod.length != 0) {
        var digitsPUC = pucCod[0].text;
        var formatPuc = true;
        if (arrData[4] == 'saldo_anterior') {
          if (arrData[5]) {//detalle a puc 8d?
            if (digitsPUC.length != 8) {
              formatPuc = false;
            }
          }
        } else {
          if (paramPuc8D == 'T') {//detalle a puc 8d?
            if (digitsPUC.length != 8) {
              formatPuc = false;
            }
          }
        }


        if (formatPuc && digitsPUC.charAt(0) == paramPUC) {
          pucResult = digitsPUC;
        } else {
          //log.debug('[map] Alert', 'La cuenta de ID ' + accountId + ' no tiene PUC de 6D dentro del rango.');
        }

      } else {
        //log.debug('[map] Alert', 'La cuenta de ID ' + accountId + ' no tiene configurado un puc de 6 digitos.');
      }

      return pucResult;
    }

    function removeSpecialCharacter(initialText) {
      var textResult = '';
      var regex = /\,|\|/gi;
      textResult = initialText.replace(regex, ' ');

      return textResult;
    }

    function actualizarThirdProc(anioProcesado) {

      var record = recordModulo.create({
        type: 'customrecord_lmry_co_terceros_procesados',
      });

      if (featuresubs || featuresubs == 'T') {
        record.setValue({
          fieldId: 'custrecord_lmry_co_subsi_procesado',
          value: '' + paramSubsidy
        });
      } else {

        var configpage = config.load({
          type: config.Type.COMPANY_INFORMATION
        });
        var idProce = configpage.getValue('id');

        record.setValue({
          fieldId: 'custrecord_lmry_co_subsi_procesado',
          value: '' + idProce
        });
      }

      record.setValue({
        fieldId: 'custrecord_lmry_co_year_procesado',
        value: anioProcesado
      });

      record.setValue({
        fieldId: 'custrecord_lmry_co_puc_procesado',
        value: '' + paramPUC
      });

      if (feamultibook || feamultibook == 'T') {
        record.setValue({
          fieldId: 'custrecord_lmry_co_multibook_procesado',
          value: '' + paramMultibook
        });
      }

      record.save();
      log.debug('Se actualizo third processed', 'anio: ' + anioProcesado + ' puc: ' + paramPUC);

    }

    function actualizarThirdData(arrTemp, puc) {

      var record = recordModulo.create({
        type: 'customrecord_lmry_co_terceros_data',
      });
      // 7. PUC 6
      record.setValue({
        fieldId: 'custrecord_lmry_co_terceros_puc6',
        value: '' + puc
      });
      // 8. AJUSTE
      if (arrTemp[4] == 'F') {
        record.setValue({
          fieldId: 'custrecord_lmry_co_terceros_adjust',
          value: false
        });
      } else {
        record.setValue({
          fieldId: 'custrecord_lmry_co_terceros_adjust',
          value: true
        });
      }
      // 0. Account
      record.setValue({
        fieldId: 'custrecord_lmry_co_terceros_account',
        value: arrTemp[0]
      });
      // 1. Debit
      var debit = 0;
      if (arrTemp[1] != null && arrTemp[1] != '') {
        debit = arrTemp[1];
      }
      record.setValue({
        fieldId: 'custrecord_lmry_co_terceros_debit',
        value: debit
      });
      // 2. Credit
      var credit = 0;
      if (arrTemp[2] != null && arrTemp[2] != '') {
        credit = arrTemp[2];
      }
      record.setValue({
        fieldId: 'custrecord_lmry_co_terceros_credit',
        value: credit
      });
      // 3. Entity
      var json_entity = {};
      var flag_entity = ObtenerEntidad(arrTemp[3]);

      if (flag_entity) {
        json_entity.name = removeSpecialCharacter(entity_name);
        json_entity.nit = entity_nit;
        json_entity.internalid = arrTemp[3];

        record.setValue({
          fieldId: 'custrecord_lmry_co_terceros_entity',
          value: JSON.stringify(json_entity)
        });
      }
      // 4. Year
      record.setValue({
        fieldId: 'custrecord_lmry_co_terceros_year',
        value: arrTemp[6]
      });
      // 5. Multibook
      if (feamultibook || feamultibook == 'T') {
        record.setValue({
          fieldId: 'custrecord_lmry_co_terceros_multibook',
          value: '' + paramMultibook
        });
      }
      // 6. Subsidiary
      if (featuresubs || featuresubs == 'T') {
        record.setValue({
          fieldId: 'custrecord_lmry_co_terceros_subsi',
          value: '' + paramSubsidy
        });
      }

      var id = record.save();
      //log.debug('Se actualizo third data', 'entity: '+JSON.stringify(json_entity));
    }

    function AgruparPorCuenta(ArrTemp) {
      var ArrReturn = new Array();

      ArrReturn.push(ArrTemp[0]);

      for (var i = 1; i < ArrTemp.length; i++) {
        var intLength = ArrReturn.length;
        for (var j = 0; j < intLength; j++) {
          //if(ArrTemp[i][0] == ArrReturn[j][0] && ArrTemp[i][3].trim() == ArrReturn[j][3].trim()){
          if (ArrTemp[i][0] == ArrReturn[j][0] && ArrTemp[i][3] == ArrReturn[j][3]) {
            ArrReturn[j][1] = Math.abs(ArrReturn[j][1]) + Math.abs(ArrTemp[i][1]);
            ArrReturn[j][2] = Math.abs(ArrReturn[j][2]) + Math.abs(ArrTemp[i][2]);
            break;
          }
          if (j == ArrReturn.length - 1) {
            ArrReturn.push(ArrTemp[i]);
          }
        }
      }

      return ArrReturn;
    }

    function ObtenerEntidad(paramEntity) {
      try {
        if (paramEntity != null && paramEntity != '') {
          var entity_customer_temp = search.lookupFields({
            type: search.Type.CUSTOMER,
            id: Number(paramEntity),
            columns: ['entityid', 'firstname', 'lastname', 'companyname', 'internalid', 'vatregnumber']
          });

          var entity_id;

          entity_nit = entity_customer_temp.vatregnumber;

          if (entity_customer_temp.internalid != null) {
            entity_id = (entity_customer_temp.internalid)[0].value;
          }

          entity_name = entity_customer_temp.firstname + ' ' + entity_customer_temp.lastname;

          if ((entity_customer_temp.firstname == null || entity_customer_temp.firstname == '') && (entity_customer_temp.lastname == null || entity_customer_temp.lastname == '') && entity_name.trim() == '') {
            entity_name = entity_customer_temp.companyname;

            if (entity_name == null && entity_name.trim() == '') {
              entity_name = entity_customer_temp.entityid;
            }
          }

          if (entity_id != null) {
            return true;
          } else {
            var entity_vendor_temp = search.lookupFields({
              type: search.Type.VENDOR,
              id: paramEntity,
              columns: ['entityid', 'firstname', 'lastname', 'companyname', 'internalid', 'vatregnumber']
            });

            entity_nit = entity_vendor_temp.vatregnumber;

            if (entity_vendor_temp.internalid != null) {
              entity_id = (entity_vendor_temp.internalid)[0].value;
            }

            entity_name = entity_vendor_temp.firstname + ' ' + entity_vendor_temp.lastname;

            if ((entity_vendor_temp.firstname == null || entity_vendor_temp.firstname == '') && (entity_vendor_temp.lastname == null || entity_vendor_temp.lastname == '') && entity_name.trim() == '') {
              entity_name = entity_vendor_temp.companyname;

              if (entity_name == null && entity_name.trim() == '') {
                entity_name = entity_vendor_temp.entityid;
              }
            }

            if (entity_id != null) {
              return true;
            } else {
              var entity_employee_temp = search.lookupFields({
                type: search.Type.EMPLOYEE,
                id: paramEntity,
                columns: ['entityid', 'firstname', 'lastname', 'internalid', 'custentity_lmry_sv_taxpayer_number']
              });

              entity_nit = entity_employee_temp.custentity_lmry_sv_taxpayer_number;

              if (entity_employee_temp.internalid != null) {
                entity_id = (entity_employee_temp.internalid)[0].value;
              }

              entity_name = entity_employee_temp.firstname + ' ' + entity_employee_temp.lastname;

              if (entity_name == null && entity_name.trim() == '') {
                entity_name = entity_employee_temp.entityid;
              }

              if (entity_id != null) {
                return true;
              } else {
                var otherNameRcd = recordModulo.load({
                  type: search.Type.OTHER_NAME,
                  id: paramEntity
                });

                var entityidField = otherNameRcd.getValue({
                  fieldId: 'entityid'
                });

                var vatregnumberField = otherNameRcd.getValue({
                  fieldId: 'vatregnumber'
                });

                var ispersonField = otherNameRcd.getValue({
                  fieldId: 'isperson'
                });

                var firstnameField = otherNameRcd.getValue({
                  fieldId: 'firstname'
                });

                var lastnameField = otherNameRcd.getValue({
                  fieldId: 'lastname'
                });

                var companynameField = otherNameRcd.getValue({
                  fieldId: 'companyname'
                });

                var internalidField = otherNameRcd.getValue({
                  fieldId: 'id'
                });

                entity_nit = vatregnumberField;

                if (internalidField != null) {
                  entity_id = internalidField;
                }

                if (ispersonField == true || ispersonField == 'T') {
                  entity_name = firstnameField + ' ' + lastnameField;
                } else {
                  entity_name = companynameField;
                }

                if (entity_id != null) {
                  entityOtherName = true;
                  return true;
                } else {
                  return false;
                }
              }
            }
          }
        } else {
          return false;
        }
      } catch (err) {
        log.error('err', err);
        log.error('paramEntity', paramEntity);
        return false;
      }
    }

    function ConvertirAString(ArrTemp) {
      var str_return = '';

      for (var i = 0; i < ArrTemp.length; i++) {
        str_return += ArrTemp[i].join('|') + '\r\n';
      }

      return str_return;
    }

    function LanzarSchedule(idfile) {
      var params = {};
      log.debug('lanzando schedule', paramPUC);
      params['custscript_lmry_co_terce_schdl_recordid'] = paramRecordID;
      params['custscript_lmry_co_terce_schdl_period'] = paramPeriod;
      params['custscript_lmry_co_terce_schdl_fileid'] = idfile;
      params['custscript_lmry_co_terce_schdl_lastpuc'] = paramPUC;
      params['custscript_lmry_co_terce_schdl_adjust'] = paramAdjustment;
      params['custscript_lmry_co_terce_schdl_opbalance'] = paramOpenBalance;
      params['custscript_lmry_co_terce_schdl_8d'] = paramPuc8D;

      if (featuresubs) {
        params['custscript_lmry_co_terce_schdl_subsi'] = paramSubsidy;
      }

      if (feamultibook) {
        params['custscript_lmry_co_terce_schdl_multi'] = paramMultibook
      }

      if (paramEntity != null) {
        params['custscript_lmry_co_terce_schdl_entity'] = paramEntity;
      }

      if (paramPeriodFin != null) {
        params['custscript_lmry_co_terce_schdl_periodfin'] = paramPeriodFin;
      }

      var RedirecSchdl = task.create({
        taskType: task.TaskType.SCHEDULED_SCRIPT,
        scriptId: 'customscript_lmry_co_bcmp_ter_schdl_v2_0',
        deploymentId: 'customdeploy_lmry_co_bcmp_ter_schdl_v2_0',
        params: params
      });

      RedirecSchdl.submit();
    }

    function savefile(Final_string) {
      var FolderId = objContext.getParameter({
        name: 'custscript_lmry_file_cabinet_rg_co'
      });

      // Almacena en la carpeta de Archivos Generados
      if (FolderId != '' && FolderId != null) {
        var Final_NameFile = 'TERCEROS_ADISTEC_MAPREDUCE_LMRY' + '.txt';
        // Crea el archivo.xls
        var file = fileModulo.create({
          name: Final_NameFile,
          fileType: fileModulo.Type.PLAINTEXT,
          contents: Final_string,
          folder: FolderId
        });

        var idfile = file.save(); // Termina de grabar el archivo
        return idfile;
      }
    }

    function ObtenerArregloFinalSeisDigitos(ArrSaldoAnterior, ArrMovimientos) {
      /*
       * SEARCH
       * 0.  cuenta
       * 1.  denominacion
       * 2.  sum debitos
       * 3.  sum credito
       * 4.  cuenta 4 digitos
       * 5.  denominacion 4 digitos
       * 6.  cuenta 2 digitos
       * 7.  denominacion 2 digitos
       * 8.  cuenta 1 digito
       * 9.  denominacion 1 digito
       * 10. entidad
       */

      var arr_final = new Array();
      var cont = 0;

      if (ArrSaldoAnterior != null && ArrSaldoAnterior.length != 0 && ArrMovimientos != null && ArrMovimientos.length != 0) {
        for (var i = 0; i < ArrSaldoAnterior.length; i++) {
          for (var j = 0; j < ArrMovimientos.length; j++) {
            if (ArrSaldoAnterior[i][0] == ArrMovimientos[j][0] && ArrSaldoAnterior[i][3].trim() == ArrMovimientos[j][3].trim()) {
              var sub_array = new Array();

              sub_array[0] = ArrSaldoAnterior[i][0];

              var saldo_anterior = Math.abs(Number(ArrSaldoAnterior[i][1])) - Math.abs(Number(ArrSaldoAnterior[i][2]));

              if (saldo_anterior > 0) {
                sub_array[1] = Math.abs(saldo_anterior);
                sub_array[2] = 0.0;
              } else if (saldo_anterior < 0) {
                sub_array[1] = 0.0;
                sub_array[2] = Math.abs(saldo_anterior);
              } else if (saldo_anterior == 0) {
                sub_array[1] = 0.0;
                sub_array[2] = 0.0;
              }

              var movimientos = Math.abs(Number(ArrMovimientos[j][1])) - Math.abs(Number(ArrMovimientos[j][2]));

              /*if (movimientos > 0) {
                  sub_array[3] = Math.abs(movimientos);
                  sub_array[4] = 0.0;
              } else if (movimientos < 0) {
                  sub_array[3] = 0.0;
                  sub_array[4] = Math.abs(movimientos);
              } else if (movimientos == 0) {
                  sub_array[3] = 0.0;
                  sub_array[4] = 0.0;
              }*/

              sub_array[3] = Math.abs(Number(ArrMovimientos[j][1]));
              sub_array[4] = Math.abs(Number(ArrMovimientos[j][2]));

              var nuevoSaldo = Math.abs(Number(ArrSaldoAnterior[i][1])) - Math.abs(Number(ArrSaldoAnterior[i][2])) + Math.abs(Number(ArrMovimientos[j][1])) - Math.abs(Number(ArrMovimientos[j][2]));

              if (nuevoSaldo > 0) {
                sub_array[5] = Math.abs(nuevoSaldo);
                sub_array[6] = 0.0;
              } else if (nuevoSaldo < 0) {
                sub_array[5] = 0.0;
                sub_array[6] = Math.abs(nuevoSaldo);
              } else if (nuevoSaldo == 0) {
                sub_array[5] = 0.0;
                sub_array[6] = 0.0;
              }

              sub_array[7] = ArrSaldoAnterior[i][3];

              if (!(Math.abs(ArrSaldoAnterior[i][1]) == 0 && Math.abs(ArrSaldoAnterior[i][2]) == 0 &&
                  Math.abs(ArrMovimientos[j][1]) == 0 && Math.abs(ArrMovimientos[j][2]) == 0)) {
                arr_final[cont] = sub_array;
                cont++;
              }

              break;
            } else {
              if (j == ArrMovimientos.length - 1) {
                var sub_array_2 = new Array();
                sub_array_2[0] = ArrSaldoAnterior[i][0];

                sub_array_2[1] = 0.0;
                sub_array_2[2] = 0.0;
                sub_array_2[3] = 0.0;
                sub_array_2[4] = 0.0;

                var saldo_anterior = Math.abs(Number(ArrSaldoAnterior[i][1])) - Math.abs(Number(ArrSaldoAnterior[i][2]));

                if (saldo_anterior > 0) {
                  sub_array_2[1] = Math.abs(saldo_anterior);
                  sub_array_2[2] = 0.0;
                } else if (saldo_anterior < 0) {
                  sub_array_2[1] = 0.0;
                  sub_array_2[2] = Math.abs(saldo_anterior);
                } else if (saldo_anterior == 0) {
                  sub_array_2[1] = 0.0;
                  sub_array_2[2] = 0.0;
                }

                sub_array_2[3] = 0.0;
                sub_array_2[4] = 0.0;

                var nuevoSaldo = Math.abs(Number(ArrSaldoAnterior[i][1])) - Math.abs(Number(ArrSaldoAnterior[i][2]));

                if (nuevoSaldo > 0) {
                  sub_array_2[5] = Math.abs(nuevoSaldo);
                  sub_array_2[6] = 0.0;
                } else if (nuevoSaldo < 0) {
                  sub_array_2[5] = 0.0;
                  sub_array_2[6] = Math.abs(nuevoSaldo);
                } else if (nuevoSaldo == 0) {
                  sub_array_2[5] = 0.0;
                  sub_array_2[6] = 0.0;
                }

                sub_array_2[7] = ArrSaldoAnterior[i][3];


                if (Number(ArrSaldoAnterior[i][1]) != 0 || Number(ArrSaldoAnterior[i][2]) != 0) {
                  arr_final[cont] = sub_array_2;
                  cont++;
                }

                break;
              }
            }
          }
        }

        for (var i = 0; i < ArrMovimientos.length; i++) {
          for (var j = 0; j < ArrSaldoAnterior.length; j++) {
            if (ArrSaldoAnterior[j][0] == ArrMovimientos[i][0]) {
              if (ArrSaldoAnterior[j][3].trim() != ArrMovimientos[i][3].trim()) {
                if (j == ArrSaldoAnterior.length - 1) {
                  var sub = new Array();
                  sub[0] = ArrMovimientos[i][0];

                  sub[1] = 0.0;
                  sub[2] = 0.0;

                  var movimientos = Math.abs(Number(ArrMovimientos[i][1])) - Math.abs(Number(ArrMovimientos[i][2]));

                  /*if (movimientos > 0) {
                      sub[3] = Math.abs(movimientos);
                      sub[4] = 0.0;
                  } else if (movimientos < 0) {
                      sub[3] = 0.0;
                      sub[4] = Math.abs(movimientos);
                  } else if (movimientos == 0.0) {
                      sub[3] = 0.0;
                      sub[4] = 0.0;
                  }*/

                  sub[3] = Math.abs(Number(ArrMovimientos[i][1]));
                  sub[4] = Math.abs(Number(ArrMovimientos[i][2]));

                  var nuevo_saldo = Math.abs(Number(ArrMovimientos[i][1])) - Math.abs(Number(ArrMovimientos[i][2]));

                  if (nuevo_saldo > 0) {
                    sub[5] = Math.abs(nuevo_saldo);
                    sub[6] = 0.0;
                  } else if (nuevo_saldo < 0) {
                    sub[5] = 0.0;
                    sub[6] = Math.abs(nuevo_saldo);
                  } else if (nuevo_saldo == 0.0) {
                    sub[5] = 0.0;
                    sub[6] = 0.0;
                  }

                  sub[7] = ArrMovimientos[i][3];

                  if (Math.abs(Number(ArrMovimientos[i][1])) != 0 || Math.abs(Number(ArrMovimientos[i][2])) != 0) {
                    arr_final[cont] = sub;
                    cont++;
                  }

                  break;
                }
              } else {
                break;
              }
            } else {
              if (j == ArrSaldoAnterior.length - 1) {
                var sub = new Array();
                sub[0] = ArrMovimientos[i][0];

                sub[1] = 0.0;
                sub[2] = 0.0;

                var movimientos = Math.abs(Number(ArrMovimientos[i][1])) - Math.abs(Number(ArrMovimientos[i][2]));

                /*if (movimientos > 0) {
                    sub[3] = Math.abs(movimientos);
                    sub[4] = 0.0;
                } else if (movimientos < 0) {
                    sub[3] = 0.0;
                    sub[4] = Math.abs(movimientos);
                } else if (movimientos == 0) {
                    sub[3] = 0.0;
                    sub[4] = 0.0;
                }*/

                sub[3] = Math.abs(Number(ArrMovimientos[i][1]));
                sub[4] = Math.abs(Number(ArrMovimientos[i][2]));

                var nuevo_saldo = Math.abs(Number(ArrMovimientos[i][1])) - Math.abs(Number(ArrMovimientos[i][2]));

                if (nuevo_saldo > 0) {
                  sub[5] = Math.abs(nuevo_saldo);
                  sub[6] = 0.0;
                } else if (nuevo_saldo < 0) {
                  sub[5] = 0.0;
                  sub[6] = Math.abs(nuevo_saldo);
                } else if (nuevo_saldo == 0.0) {
                  sub[5] = 0.0;
                  sub[6] = 0.0;
                }
                sub[7] = ArrMovimientos[i][3];

                if (Math.abs(Number(ArrMovimientos[i][1])) != 0 || Math.abs(Number(ArrMovimientos[i][2])) != 0) {
                  arr_final[cont] = sub;
                  cont++;
                }

                break;
              }
            }
          }
        }
      } else if (ArrMovimientos != null && ArrMovimientos.length != 0) {
        for (var i = 0; i < ArrMovimientos.length; i++) {
          var sub = new Array();

          sub[0] = ArrMovimientos[i][0];

          sub[1] = 0.0;
          sub[2] = 0.0;

          var movimientos = Math.abs(ArrMovimientos[i][1]) - Math.abs(ArrMovimientos[i][2]);

          /*if (movimientos > 0) {
              sub[3] = Math.abs(movimientos);
              sub[4] = 0.0;
          } else if (movimientos < 0) {
              sub[3] = 0.0;
              sub[4] = Math.abs(movimientos);
          } else if (movimientos == 0) {
              sub[3] = 0.0;
              sub[4] = 0.0;
          }*/

          sub[3] = Math.abs(ArrMovimientos[i][1]);
          sub[4] = Math.abs(ArrMovimientos[i][2]);

          var nuevo_saldo = Number(ArrMovimientos[i][1]) - Number(ArrMovimientos[i][2]);

          if (nuevo_saldo > 0) {
            sub[5] = Math.abs(nuevo_saldo);
            sub[6] = 0.0;
          } else if (nuevo_saldo < 0) {
            sub[5] = 0.0;
            sub[6] = Math.abs(nuevo_saldo);
          } else if (nuevo_saldo == 0.0) {
            sub[5] = 0.0;
            sub[6] = 0.0;
          }
          sub[7] = ArrMovimientos[i][3];

          arr_final[cont] = sub;
          cont++;
        }
      } else if (ArrSaldoAnterior != null && ArrSaldoAnterior.length != 0) {
        for (var i = 0; i < ArrSaldoAnterior.length; i++) {
          var sub = new Array();

          sub[0] = ArrSaldoAnterior[i][0];

          var saldo_anterior = Number(ArrSaldoAnterior[i][1]) - Number(ArrSaldoAnterior[i][2]);

          if (saldo_anterior > 0) {
            sub[1] = Math.abs(saldo_anterior);
            sub[2] = 0.0;
          } else if (saldo_anterior < 0) {
            sub[1] = 0.0;
            sub[2] = Math.abs(saldo_anterior);
          } else if (saldo_anterior == 0.0) {
            sub[1] = 0.0;
            sub[2] = 0.0;
          }
          sub[3] = 0.0;
          sub[4] = 0.0;

          var nuevo_saldo = Number(ArrSaldoAnterior[i][1]) - Number(ArrSaldoAnterior[i][2]);

          if (nuevo_saldo > 0) {
            sub[5] = Math.abs(nuevo_saldo);
            sub[6] = 0.0;
          } else if (nuevo_saldo < 0) {
            sub[5] = 0.0;
            sub[6] = Math.abs(nuevo_saldo);
          } else if (nuevo_saldo == 0.0) {
            sub[5] = 0.0;
            sub[6] = 0.0;
          }
          sub[7] = ArrSaldoAnterior[i][3];

          arr_final[cont] = sub;
          cont++;
        }
      } else {
        flagEmpty = true;
      }


      return arr_final;
    }

    function JuntarYAgruparArreglos(dataAnt, dataRestante) {
      if (dataAnt.length != 0 && dataRestante.length != 0) {
        var ArrReturn = new Array();
        var cont = 0;
        var Length = dataAnt.length;

        for (var i = 0; i < dataRestante.length; i++) {
          for (var j = 0; j < Length; j++) {
            if (dataRestante[i][0] == dataAnt[j][0] && dataRestante[i][3].trim() == dataAnt[j][3].trim()) {
              dataAnt[j][1] = Number(dataAnt[j][1]) + Number(dataRestante[i][1]);
              dataAnt[j][2] = Number(dataAnt[j][2]) + Number(dataRestante[i][2]);
              break;
            }

            if (j == Length - 1) {
              dataAnt.push(dataRestante[i]);
            }
          }
        }

        return dataAnt;
      } else if (dataAnt.length != 0) {
        return dataAnt;
      } else if (dataRestante.length != 0) {
        return dataRestante;
      }
    }

    function ObtenerSaldoAntRecord(firstYear, lastYear) {
      var intDMinReg = 0;
      var intDMaxReg = 1000;

      var DbolStop = false;
      var ArrReturn = new Array();
      var cont = 0;

      var busqueda = search.create({
        type: 'customrecord_lmry_co_terceros_data',
        filters: [
          search.createFilter({
            name: 'custrecord_lmry_co_terceros_year',
            operator: search.Operator.GREATERTHANOREQUALTO,
            values: [firstYear]
          }),
          search.createFilter({
            name: 'custrecord_lmry_co_terceros_year',
            operator: search.Operator.LESSTHANOREQUALTO,
            values: [lastYear]
          }),
          search.createFilter({
            name: 'formulanumeric',
            formula: 'CASE WHEN {custrecord_lmry_co_terceros_debit} - {custrecord_lmry_co_terceros_credit} <> 0 THEN 1 ELSE 0 END',
            operator: search.Operator.EQUALTO,
            values: [1]
          }),
          search.createFilter({
            name: 'custrecord_lmry_co_terceros_puc6',
            operator: search.Operator.STARTSWITH,
            values: [paramPUC]
          })
        ],
        columns: [
          search.createColumn({
            name: 'custrecord_lmry_co_terceros_account',
            summary: 'GROUP',
            sort: search.Sort.ASC
          }),
          search.createColumn({
            name: 'custrecord_lmry_co_terceros_debit',
            summary: 'SUM'
          }),
          search.createColumn({
            name: 'custrecord_lmry_co_terceros_credit',
            summary: 'SUM'
          }),
          search.createColumn({
            name: 'custrecord_lmry_co_terceros_entity',
            summary: 'GROUP'
          })
        ]
      });

      if (paramPuc8D == 'T') {
        var pucFilter = search.createFilter({
          name: 'formulanumeric',
          formula: 'LENGTH({custrecord_lmry_co_terceros_puc6})',
          operator: search.Operator.EQUALTO,
          values: 8
        });
        busqueda.filters.push(pucFilter);
      }

      if (feamultibook || feamultibook == 'T') {
        var multibookFilter = search.createFilter({
          name: 'custrecord_lmry_co_terceros_multibook',
          operator: search.Operator.IS,
          values: [paramMultibook]
        });
        busqueda.filters.push(multibookFilter);
      }

      if (featuresubs || featuresubs == 'T') {
        var subsidiaryFilter = search.createFilter({
          name: 'custrecord_lmry_co_terceros_subsi',
          operator: search.Operator.IS,
          values: [paramSubsidy]
        });
        busqueda.filters.push(subsidiaryFilter);
      }

      var savedsearch = busqueda.run();

      while (!DbolStop) {
        var objResult = savedsearch.getRange(intDMinReg, intDMaxReg);

        if (objResult != null) {

          if (objResult.length != 1000) {
            DbolStop = true;
          }

          for (var i = 0; i < objResult.length; i++) {
            var columns = objResult[i].columns;
            var arrAuxiliar = new Array();
            // 0. Account
            if (objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '- None -' && objResult[i].getValue(columns[0]) != 'NaN' && objResult[i].getValue(columns[0]) != 'undefined') {
              arrAuxiliar[0] = objResult[i].getValue(columns[0]);
            } else {
              arrAuxiliar[0] = '';
            }
            // 1. Debit
            if (objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '- None -' && objResult[i].getValue(columns[1]) != 'NaN' && objResult[i].getValue(columns[1]) != 'undefined') {
              arrAuxiliar[1] = objResult[i].getValue(columns[1]);
            } else {
              arrAuxiliar[1] = '';
            }
            // 2. Credit
            if (objResult[i].getValue(columns[2]) != null && objResult[i].getValue(columns[2]) != '- None -' && objResult[i].getValue(columns[2]) != 'NaN' && objResult[i].getValue(columns[2]) != 'undefined') {
              arrAuxiliar[2] = objResult[i].getValue(columns[2]);
            } else {
              arrAuxiliar[2] = '';
            }
            // 3. Entity
            if (objResult[i].getValue(columns[3]) != null && objResult[i].getValue(columns[3]) != '- None -' && objResult[i].getValue(columns[3]) != 'NaN' && objResult[i].getValue(columns[3]) != 'undefined') {
              arrAuxiliar[3] = objResult[i].getValue(columns[3]);
            } else {
              arrAuxiliar[3] = '';
            }

            if (paramEntity != null) {
              if (arrAuxiliar[3] != '') {
                var entityJSON = JSON.parse(arrAuxiliar[3]);

                if (entityJSON.internalid == paramEntity) {
                  ArrReturn[cont] = arrAuxiliar;
                  cont++;
                }
              }
            } else {
              ArrReturn[cont] = arrAuxiliar;
              cont++;
            }
          }

          if (!DbolStop) {
            intDMinReg = intDMaxReg;
            intDMaxReg += 1000;
          }

        } else {
          DbolStop = true;
        }
      }

      return ArrReturn;
    }

    function ObtenerPeriodosDelAño(periodsData) {
      var arrReturn = new Array();

      for (var i = 0; i < periodsData.length; i++) {
        var tempYear = format.parse({
          value: periodsData[i][1],
          type: format.Type.DATE
        }).getFullYear();

        var tempMonth = format.parse({
          value: periodsData[i][1],
          type: format.Type.DATE
        }).getMonth();

        if (tempYear == periodYearIni && tempMonth < periodMonthIni) {
          var arr = new Array();
          arr[0] = periodsData[i][0];
          arr[1] = periodsData[i][1];
          arrReturn.push(arr);

        } else if (tempYear == periodYearIni && tempMonth == periodMonthIni) {
          if ((periodsData[i][2] != true && periodsData[i][2] != 'T') && (periodsData[i][0] != paramPeriod)) {
            var arr = new Array();
            arr[0] = periodsData[i][0];
            arr[1] = periodsData[i][1];
            arrReturn.push(arr);
          }
        }

      }

      arrReturn = OrdenarPeriodosPorMes(arrReturn);
      return arrReturn;
    }

    function OrdenarPeriodosPorMes(arrTemporal) {
      var swapped;

      do {
        swapped = false;
        for (var i = 0; i < arrTemporal.length - 1; i++) {
          var a = format.parse({
            value: arrTemporal[i][1],
            type: format.Type.DATE
          }).getMonth();

          var b = format.parse({
            value: arrTemporal[i + 1][1],
            type: format.Type.DATE
          }).getMonth();

          if (Number(a) > Number(b)) {
            var temp = new Array();
            temp = arrTemporal[i];
            arrTemporal[i] = arrTemporal[i + 1];
            arrTemporal[i + 1] = temp;
            swapped = true;
          }
        }

      } while (swapped);

      return arrTemporal;
    }

    function ObtenerPeriodos() {
      var intDMinReg = 0;
      var intDMaxReg = 1000;

      var DbolStop = false;
      var ArrReturn = new Array();
      var cont = 0;

      var busqueda = search.create({
        type: search.Type.ACCOUNTING_PERIOD,
        filters: [
          search.createFilter({
            name: 'isquarter',
            operator: search.Operator.IS,
            values: ['F']
          }),
          search.createFilter({
            name: 'isinactive',
            operator: search.Operator.IS,
            values: ['F']
          }),
          search.createFilter({
            name: 'isyear',
            operator: search.Operator.IS,
            values: ['F']
          })
        ],
        columns: ['internalid', 'startdate', 'isadjust']
      });

      if (featureCalendars || featureCalendars == 'T') {
        var calendarFilter = search.createFilter({
          name: 'fiscalcalendar',
          operator: search.Operator.IS,
          values: calendarSubsi
        });
        busqueda.filters.push(calendarFilter);
      }

      if (paramAdjustment == 'F') {
        var adjustFilter = search.createFilter({
          name: 'isadjust',
          operator: search.Operator.IS,
          values: false
        });
        busqueda.filters.push(adjustFilter);
      }

      var savedsearch = busqueda.run();

      while (!DbolStop) {
        var objResult = savedsearch.getRange(intDMinReg, intDMaxReg);

        if (objResult != null) {

          if (objResult.length != 1000) {
            DbolStop = true;
          }

          for (var i = 0; i < objResult.length; i++) {
            var columns = objResult[i].columns;
            var arrAuxiliar = new Array();
            // 0. Internal ID
            arrAuxiliar[0] = objResult[i].getValue(columns[0]);
            // 1. Start Date
            if (objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '- None -' && objResult[i].getValue(columns[1]) != 'NaN' && objResult[i].getValue(columns[1]) != 'undefined') {
              arrAuxiliar[1] = objResult[i].getValue(columns[1]);
            } else {
              arrAuxiliar[1] = '';
            }
            // 2. IS ADJUST
            arrAuxiliar[2] = objResult[i].getValue(columns[2]);

            ArrReturn[cont] = arrAuxiliar;
            cont++;
          }

          if (!DbolStop) {
            intDMinReg = intDMaxReg;
            intDMaxReg += 1000;
          }

        } else {
          DbolStop = true;
        }
      }

      return ArrReturn;
    }

    function OrdenarAños(arrTemporal) {
      var swapped;
      do {
        swapped = false;
        for (var i = 0; i < arrTemporal.length - 1; i++) {
          if (arrTemporal[i][1] > arrTemporal[i + 1][1]) {
            var temp = new Array();
            temp = arrTemporal[i];
            arrTemporal[i] = arrTemporal[i + 1];
            arrTemporal[i + 1] = temp;
            swapped = true;
          }
        }

      } while (swapped);

      return arrTemporal;
    }

    function ObtenerAñosProcesados() {
      var intDMinReg = 0;
      var intDMaxReg = 1000;
      var DbolStop = false;
      var ArrReturn = new Array();

      var busqueda = search.create({
        type: 'customrecord_lmry_co_terceros_procesados',
        filters: [
          ['isinactive', 'is', 'F']
        ],
        columns: ['internalid', 'custrecord_lmry_co_year_procesado', 'custrecord_lmry_co_multibook_procesado', 'custrecord_lmry_co_subsi_procesado', 'custrecord_lmry_co_puc_procesado']
      });

      if (feamultibook || feamultibook == 'T') {
        var multibookFilter = search.createFilter({
          name: 'custrecord_lmry_co_multibook_procesado',
          operator: search.Operator.IS,
          values: [paramMultibook]
        });
        busqueda.filters.push(multibookFilter);
      }

      if (featuresubs || featuresubs == 'T') {
        var subsidiaryFilter = search.createFilter({
          name: 'custrecord_lmry_co_subsi_procesado',
          operator: search.Operator.IS,
          values: [paramSubsidy]
        });
        busqueda.filters.push(subsidiaryFilter);
      }

      var savedsearch = busqueda.run();

      while (!DbolStop) {
        var objResult = savedsearch.getRange(intDMinReg, intDMaxReg);

        if (objResult != null) {

          if (objResult.length != 1000) {
            DbolStop = true;
          }

          for (var i = 0; i < objResult.length; i++) {
            var columns = objResult[i].columns;
            var arrAuxiliar = new Array();
            // 0. Internal ID
            arrAuxiliar[0] = objResult[i].getValue(columns[0]);
            // 1. Año
            if (objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '- None -' && objResult[i].getValue(columns[1]) != 'NaN' && objResult[i].getValue(columns[1]) != 'undefined') {
              arrAuxiliar[1] = objResult[i].getValue(columns[1]);
            } else {
              arrAuxiliar[1] = '';
            }
            // 2. Multibook
            if (objResult[i].getValue(columns[2]) != null && objResult[i].getValue(columns[2]) != '- None -' && objResult[i].getValue(columns[2]) != 'NaN' && objResult[i].getValue(columns[2]) != 'undefined') {
              arrAuxiliar[2] = objResult[i].getValue(columns[2]);
            } else {
              arrAuxiliar[2] = '';
            }
            // 3. Subsidiaria
            if (objResult[i].getValue(columns[3]) != null && objResult[i].getValue(columns[3]) != '- None -' && objResult[i].getValue(columns[3]) != 'NaN' && objResult[i].getValue(columns[3]) != 'undefined') {
              arrAuxiliar[3] = objResult[i].getValue(columns[3]);
            } else {
              arrAuxiliar[3] = '';
            }
            // 4. PUC
            if (objResult[i].getValue(columns[4]) != null && objResult[i].getValue(columns[4]) != '- None -' && objResult[i].getValue(columns[4]) != 'NaN' && objResult[i].getValue(columns[4]) != 'undefined') {
              arrAuxiliar[4] = objResult[i].getValue(columns[4]);
            } else {
              arrAuxiliar[4] = '';
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

      return ArrReturn;
    }

    function ObtenerData(periodYearIniID, type, isForRecord, paramPeriodFin) {
      var intDMinReg = 0;
      var intDMaxReg = 1000;
      var DbolStop = false;
      var ArrReturn = new Array();
      var existPuc8 = existPuc8d();

      if (paramPuc8D == 'T' && !existPuc8) {
        return ArrReturn
      }

      var savedsearch = search.load({
        /*LatamReady - CO Balance Comp Terceros Data*/
        id: 'customsearch_lmry_co_bal_comp_terc_data'
      });

      if (type != "saldo_anterior" && type != "saldo_restante") { //solo para transacciones de movimientos
        if (paramAdjustment == 'F') {
          var adjustFilter = search.createFilter({
            name: 'isadjust',
            join: 'accountingperiod',
            operator: search.Operator.IS,
            values: false
          });
          savedsearch.filters.push(adjustFilter);
        }
      }

      if (type == "saldo_anterior" || type == "saldo_restante") {
        if (featurePeriodEnd) {
          var confiPeriodEnd = search.createSetting({
            name: 'includeperiodendtransactions',
            value: 'TRUE'
          })
          savedsearch.settings.push(confiPeriodEnd);
        }
      } else {
        if (paramAdjustment == 'T') {
          if (featurePeriodEnd) {
            var confiPeriodEnd = search.createSetting({
              name: 'includeperiodendtransactions',
              value: 'TRUE'
            })
            savedsearch.settings.push(confiPeriodEnd);
          }
        }
      }

      if (featuresubs) {
        var subsidiaryFilter = search.createFilter({
          name: 'subsidiary',
          operator: search.Operator.IS,
          values: [paramSubsidy]
        });
        savedsearch.filters.push(subsidiaryFilter);
      }

      if (!isForRecord) {
        if (paramEntity != null) {
          var formulaEntity = 'CASE WHEN NVL({custcol_lmry_exp_rep_vendor_colum.internalid}, NVL({entity.id}, ' +
            'NVL({customer.internalid}, NVL({vendor.internalid}, NVL({vendorline.internalid},' +
            ' {employee.internalid}))))) = ' + paramEntity + ' THEN 1 ELSE 0 END';

          var entityFilter = search.createFilter({
            name: 'formulatext',
            formula: formulaEntity,
            operator: search.Operator.IS,
            values: [1]
          });

          savedsearch.filters.push(entityFilter);
        }
      }

      if (type == 'saldo_anterior') { //saldo de años pasados al periodo inicial de generacion
        if (featAccountingSpecial || featAccountingSpecial == 'T') {
          periodYearIniID = periodYearIniID.map(function e(p) {
            return p[0]
          });

          var periodosSTR = periodYearIniID.toString();
          log.debug('periodosSTR saldo_anterior', periodosSTR);

          var periodFilter = search.createFilter({
            name: 'formulanumeric',
            formula: 'CASE WHEN {postingperiod.id} IN (' + periodosSTR + ') THEN 1 ELSE 0 END',
            operator: search.Operator.EQUALTO,
            values: [1]
          });
          savedsearch.filters.push(periodFilter);

        } else {
          var periodFilter = search.createFilter({
            name: 'postingperiod',
            operator: search.Operator.IS,
            values: [periodYearIniID]
          });
          savedsearch.filters.push(periodFilter);
        }

      } else if (type == 'saldo_restante') { //saldo de periodos anterios al periodo inicial de generacion pero del mismo año

        periodYearIniID = periodYearIniID.map(function e(p) {
          return p[0]
        });
        var periodosSTR = periodYearIniID.toString();
        log.debug('periodosSTR saldo_restante', periodosSTR);

        var periodFilter = search.createFilter({
          name: 'formulanumeric',
          formula: 'CASE WHEN {postingperiod.id} IN (' + periodosSTR + ') THEN 1 ELSE 0 END',
          operator: search.Operator.EQUALTO,
          values: [1]
        });
        savedsearch.filters.push(periodFilter);
        /*var periodFilter = search.createFilter({
          name: 'postingperiod',
          operator: search.Operator.ANYOF,
          values: [arrTemp]
        });
        savedsearch.filters.push(periodFilter);
        */
      } else {
        /*MOVIMIENTOS*/ //saldo desde el periodo de inicio hasta el periodo final
        var finDate = null;
        if (paramPeriodFin != null) {
          finDate = periodenddateFin;
        } else {
          finDate = periodenddateIni;
        }

        log.debug('periodoInicioDate', periodstartdateIni);
        log.debug('periodoFinDate', finDate);

        var periodsId = periodsIdByDate(periodstartdateIni, finDate);
        var formulaPeriod = 'CASE WHEN {postingperiod.id} IN (' + periodsId.toString() + ') THEN 1 ELSE 0 END'
        log.debug('formulaPeriod', formulaPeriod);
        //29/12/2017
        var periodFilter = search.createFilter({
          name: 'formulanumeric',
          formula: formulaPeriod,
          operator: search.Operator.EQUALTO,
          values: [1]
        });
        savedsearch.filters.push(periodFilter);
      }

      if (feamultibook) {

        var multibookFilter = search.createFilter({
          name: 'accountingbook',
          join: 'accountingtransaction',
          operator: search.Operator.IS,
          values: [paramMultibook]
        });
        savedsearch.filters.push(multibookFilter);

        //columan5
        var columnaDebit = search.createColumn({
          name: 'formulacurrency',
          formula: "{accountingtransaction.debitamount}",
          summary: 'SUM'
        });
        savedsearch.columns.push(columnaDebit);
        //columna6
        var columnaCredit = search.createColumn({
          name: 'formulacurrency',
          formula: "{accountingtransaction.creditamount}",
          summary: 'SUM'
        });
        savedsearch.columns.push(columnaCredit);
        //columna7
        var columnaActMulti = search.createColumn({
          name: 'account',
          join: 'accountingtransaction',
          summary: 'GROUP',
          sort: search.Sort.ASC
        });
        savedsearch.columns.push(columnaActMulti);

      } else {

        if (existPuc8) {
          var pucFilter = search.createFilter({
            name: 'formulatext',
            formula: '{account.custrecord_lmry_co_puc_id}',
            operator: search.Operator.STARTSWITH,
            values: [paramPUC]
          });

        } else {
          var pucFilter = search.createFilter({
            name: 'formulatext',
            formula: '{account.custrecord_lmry_co_puc_d6_id}',
            operator: search.Operator.STARTSWITH,
            values: [paramPUC]
          });

        }
        savedsearch.filters.push(pucFilter);
      }

      var searchresult = savedsearch.run();

      while (!DbolStop) {
        var objResult = searchresult.getRange(intDMinReg, intDMaxReg);

        if (objResult != null) {

          if (objResult.length != 1000) {
            DbolStop = true;
          }

          var intLength = objResult.length;

          for (var i = 0; i < intLength; i++) {
            var columns = objResult[i].columns;
            var arr = new Array();

            if (feamultibook || feamultibook == 'T') {
              // 0. Account
              if (objResult[i].getValue(columns[7]) != null && objResult[i].getValue(columns[7]) != '- None -' && objResult[i].getValue(columns[7]) != 'NaN' && objResult[i].getValue(columns[7]) != 'undefined') {
                arr[0] = objResult[i].getValue(columns[7]);
              } else {
                arr[0] = '';
              }
              // 1. Debit
              if (objResult[i].getValue(columns[5]) != null && objResult[i].getValue(columns[5]) != '- None -' && objResult[i].getValue(columns[5]) != 'NaN' && objResult[i].getValue(columns[5]) != 'undefined') {
                arr[1] = objResult[i].getValue(columns[5]);
              } else {
                arr[1] = '';
              }
              // 2. Credit
              if (objResult[i].getValue(columns[6]) != null && objResult[i].getValue(columns[6]) != '- None -' && objResult[i].getValue(columns[6]) != 'NaN' && objResult[i].getValue(columns[6]) != 'undefined') {
                arr[2] = objResult[i].getValue(columns[6]);
              } else {
                arr[2] = '';
              }

            } else {
              // 0. Account
              if (objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '- None -' && objResult[i].getValue(columns[0]) != 'NaN' && objResult[i].getValue(columns[0]) != 'undefined') {
                arr[0] = objResult[i].getValue(columns[0]);
              } else {
                arr[0] = '';
              }
              // 1. Debit
              if (objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '- None -' && objResult[i].getValue(columns[1]) != 'NaN' && objResult[i].getValue(columns[1]) != 'undefined') {
                arr[1] = objResult[i].getValue(columns[1]);
              } else {
                arr[1] = '';
              }
              // 2. Credit
              if (objResult[i].getValue(columns[2]) != null && objResult[i].getValue(columns[2]) != '- None -' && objResult[i].getValue(columns[2]) != 'NaN' && objResult[i].getValue(columns[2]) != 'undefined') {
                arr[2] = objResult[i].getValue(columns[2]);
              } else {
                arr[2] = '';
              }
            }

            // 3. Entity
            if (objResult[i].getValue(columns[3]) != null && objResult[i].getValue(columns[3]) != '' && objResult[i].getValue(columns[3]) != '- None -') {
              arr[3] = objResult[i].getValue(columns[3]);
            } else {
              arr[3] = '';
            }

            if (isForRecord) {
              arr[4] = objResult[i].getValue(columns[4]); //True: si es en adjust, False: no es en adjust
            } else {
              arr[4] = type; //"saldo_restante" o "movimientos"
            }

            arr[5] = existPuc8;

            if (paramEntity != null) {
              if (!isForRecord) {
                if (paramEntity == arr[3]) {
                  ArrReturn.push(arr);
                }
              } else {
                ArrReturn.push(arr);
              }
            } else {
              ArrReturn.push(arr);
            }
          }

          if (!DbolStop) {
            intDMinReg = intDMaxReg;
            intDMaxReg += 1000;
          }

        } else {
          DbolStop = true;
        }
      }

      return ArrReturn;
    }

    function ParametrosYFeatures() {

      if (paramPUC == null) {
        paramPUC = 1;
      }
      log.debug('parametros:', ' entity -' + paramEntity + ' Multibook -' + paramMultibook + ' recorID -' + paramRecordID + ' Subsi -' + paramSubsidy + ' periodo -' + paramPeriod + ' PUC -' + paramPUC + ' periodFIn -' + paramPeriodFin + ' adjustment -' + paramAdjustment + ' open balance -' + paramOpenBalance + ' puc 8d -' + paramPuc8D);
      var period_temp = search.lookupFields({
        type: search.Type.ACCOUNTING_PERIOD,
        id: paramPeriod,
        columns: ['startdate', 'enddate']
      });

      periodstartdateIni = period_temp.startdate;
      periodenddateIni = period_temp.enddate;

      periodYearIni = format.parse({
        value: periodstartdateIni,
        type: format.Type.DATE
      }).getFullYear();

      periodMonthIni = format.parse({
        value: periodstartdateIni,
        type: format.Type.DATE
      }).getMonth();

      if (paramPeriodFin != null && paramPeriodFin != '') {
        var period_temp_2 = search.lookupFields({
          type: search.Type.ACCOUNTING_PERIOD,
          id: paramPeriodFin,
          columns: ['startdate', 'enddate']
        });

        periodenddateFin = period_temp_2.enddate;
      }

      var licenses = libRpt.getLicenses(paramSubsidy);
      featAccountingSpecial = libRpt.getAuthorization(677, licenses);

      if (featureCalendars || featureCalendars == 'T') {
        var subsidiary = search.lookupFields({
          type: search.Type.SUBSIDIARY,
          id: paramSubsidy,
          columns: ['fiscalcalendar']
        });

        calendarSubsi = subsidiary.fiscalcalendar[0].value;
        //log.debug('calendarSubsi', calendarSubsi);
      }

    }

    function getSpecialPeriods(year) {
      var arrayResult = new Array();

      var searchSpecialPeriod = search.create({
        type: "customrecord_lmry_special_accountperiod",
        filters: [
          ["isinactive", "is", "F"]
        ],
        columns: [
          search.createColumn({
            name: "custrecord_lmry_anio_fisco",
            summary: "GROUP",
            label: "0. Latam - Year Fiscal"
          }),
          search.createColumn({
            name: "custrecord_lmry_calendar",
            summary: "GROUP",
            label: "1. Latam - Calendar"
          }),
          search.createColumn({
            name: "formulatext",
            summary: "GROUP",
            formula: "{custrecord_lmry_accounting_period.periodname}",
            label: "2. Formula (Text)"
          }),
          search.createColumn({
            name: "formulanumeric",
            summary: "GROUP",
            formula: "{custrecord_lmry_accounting_period.id}",
            label: "3. Formula (Numeric)"
          }),
          search.createColumn({
            name: "custrecord_lmry_date_ini",
            summary: "GROUP",
            label: "4. Latam - Date Start",
            sort: search.Sort.ASC,
          })
        ]
      });

      if (year != null && year != '') {
        var filtroYear = search.createFilter({
          name: 'custrecord_lmry_anio_fisco',
          operator: search.Operator.IS,
          values: year
        });
        searchSpecialPeriod.filters.push(filtroYear);
      }

      var pagedData = searchSpecialPeriod.runPaged({
        pageSize: 1000
      });

      pagedData.pageRanges.forEach(function(pageRange) {
        page = pagedData.fetch({
          index: pageRange.index
        });

        page.data.forEach(function(result) {
          columns = result.columns;
          var calendar = result.getValue(columns[1]);
          var periodName = result.getValue(columns[2]);
          var periodId = result.getValue(columns[3]);
          var startDate = result.getValue(columns[4]);
          var temporal = [periodId, startDate, periodName];

          if (calendarSubsi != null && calendarSubsi != '' && calendarSubsi != '- None -') {
            if (calendar != null && calendar != '' && calendar != '- None -') {
              calendar = JSON.parse(calendar);
              if (calendar.id == calendarSubsi) {
                arrayResult.push(temporal);
              }
            } else {
              log.debug('No existe calendar en Special Period');
            }
          } else {
            arrayResult.push(temporal);
          }

        })
      });

      log.debug('getSpecialPeriods', arrayResult);
      return arrayResult;
    }

    function getFiscalYearsSpecialPeriod() {
      var arrayResult = new Array();

      var searchSpecialPeriod = search.create({
        type: "customrecord_lmry_special_accountperiod",
        filters: [
          ["isinactive", "is", "F"]
        ],
        columns: [
          search.createColumn({
            name: "custrecord_lmry_anio_fisco",
            summary: "GROUP",
            sort: search.Sort.ASC,
            label: "Latam - Year Fiscal"
          }),
          search.createColumn({
            name: "custrecord_lmry_calendar",
            summary: "GROUP",
            label: "Latam - Calendar"
          })
        ]
      });

      var pagedData = searchSpecialPeriod.runPaged({
        pageSize: 1000
      });

      pagedData.pageRanges.forEach(function(pageRange) {
        page = pagedData.fetch({
          index: pageRange.index
        });

        page.data.forEach(function(result) {
          columns = result.columns;
          var calendar = result.getValue(columns[1]);
          //log.debug('calendar',calendar);
          var anio = result.getValue(columns[0]);

          if (anio < periodYearIni) {
            if (calendarSubsi != null && calendarSubsi != '' && calendarSubsi != '- None -') {
              if (calendar != null && calendar != '' && calendar != '- None -') {
                calendar = JSON.parse(calendar);
                if (calendar.id == calendarSubsi) {
                  arrayResult.push(anio);
                }
              } else {
                log.debug('No existe calendar en Special Period');
              }
            } else {
              arrayResult.push(anio);
            }

          }

        })
      });

      log.debug('años de special period', arrayResult);
      return arrayResult;
    }

    function ObtenerAñosFiscales() {
      var intDMinReg = 0;
      var intDMaxReg = 1000;
      var DbolStop = false;

      var ArrReturn = new Array();
      var cont = 0;

      var busqueda = search.create({
        type: search.Type.ACCOUNTING_PERIOD,
        filters: [
          search.createFilter({
            name: 'isyear',
            operator: search.Operator.IS,
            values: ['T']
          }),
          search.createFilter({
            name: 'isinactive',
            operator: search.Operator.IS,
            values: ['F']
          })
        ],
        columns: ['internalid', 'startdate']
      });

      if (featureCalendars || featureCalendars == 'T') {
        var calendarFilter = search.createFilter({
          name: 'fiscalcalendar',
          operator: search.Operator.IS,
          values: calendarSubsi
        });
        busqueda.filters.push(calendarFilter);
      }

      var savedsearch = busqueda.run();

      while (!DbolStop) {
        var objResult = savedsearch.getRange(intDMinReg, intDMaxReg);

        if (objResult != null) {

          if (objResult.length != 1000) {
            DbolStop = true;
          }

          for (var i = 0; i < objResult.length; i++) {
            var columns = objResult[i].columns;
            var arrAuxiliar = new Array();
            // 0. Internal ID
            if (objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '- None -' && objResult[i].getValue(columns[0]) != 'NaN' && objResult[i].getValue(columns[0]) != 'undefined') {
              arrAuxiliar[0] = objResult[i].getValue(columns[0]);
            } else {
              arrAuxiliar[0] = '';
            }
            // 1. Start Date
            if (objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '- None -' && objResult[i].getValue(columns[1]) != 'NaN' && objResult[i].getValue(columns[1]) != 'undefined') {
              arrAuxiliar[1] = objResult[i].getValue(columns[1]);
            } else {
              arrAuxiliar[1] = '';
            }

            var startDateYearTemp = format.parse({
              value: arrAuxiliar[1],
              type: format.Type.DATE
            }).getFullYear();

            arrAuxiliar[1] = startDateYearTemp;

            if (startDateYearTemp < periodYearIni) {
              ArrReturn[cont] = arrAuxiliar;
              cont++;
            }
          }

          if (!DbolStop) {
            intDMinReg = intDMaxReg;
            intDMaxReg += 1000;
          }

        } else {
          DbolStop = true;
        }
      }

      return ArrReturn;
    }

    return {
      getInputData: getInputData,
      map: map,
      summarize: summarize
    };

  });
