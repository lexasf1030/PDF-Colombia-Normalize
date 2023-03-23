/* = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =\
||   This script for customer center (Time)                     ||
||                                                              ||
||  File Name: LMRY_CO_ReporteMagAnual1012v7.1_MPRD_V2.js      ||
||                                                              ||
||  Version Date           Author        Remarks                ||
||  2.0     Marzo 23 2020  LatamReady    Use Script 2.0         ||
 \= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = */

/**
 *@NApiVersion 2.x
 *@NScriptType MapReduceScript
 *@NModuleScope Public
 */

define(['N/record', 'N/runtime', 'N/file', 'N/email', 'N/search', 'N/encode', 'N/currency',
    'N/format', 'N/log', 'N/config', 'N/xml', 'N/task', './CO_Library_Mensual/LMRY_CO_Reportes_LBRY_V2.0.js'
  ],

  function(record, runtime, file, email, search, encode, currency, format, log, config, xml, task, libreria) {
    var objContext = runtime.getCurrentScript();
    var reportName = '"Reporte de Medios Magneticos: Formulario 1012 v7.1"';
    var LMRY_script = 'LMRY_CO_ReporteMagAnual1012v7.1_MPRD_V2.js';

    var paramSubsidiaria;
    var paramMultibook;
    var paramPeriodo;
    var paramIdLog;
    var paramCont;
    var paramIdReport;
    var paramIdFeatureByVersion;
    var paramConcepto;

    var isMultibookFeature;
    var isSubsidiariaFeature;

    var companyName = '';
    var companyRuc = '';
    var ArrBank = new Array();

    var periodEndDate;
    var periodEndDateFormat;

    var periodName;
    var multibookName;
    var valorTotal = 0;
    var numeroEnvio;

    var strExcelInversiones = '';
    var strXmlInversiones = '';
    var generarXml = false;

    var language = runtime.getCurrentScript().getParameter("LANGUAGE").substring(0, 2);
    var GLOBAL_LABELS = {};

    function getInputData() {
      var jsonTransacciones = {};
      ObtenerParametrosYFeatures();
      ObtenerDatosSubsidiaria();

      ObtieneBank();
      var cuentas = ObtenerCuentas();
      log.debug("cuentas", cuentas);
      if (Object.keys(cuentas).length != 0) {
        jsonTransacciones = ObtenerInversiones(cuentas);
      }
      log.debug("jsonTransacciones", jsonTransacciones);

      return jsonTransacciones;
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

    }

    //{"recordType":"invoice","id":"2776","values":{"entity":{"value":"628","text":"Selders Distributors"},"tranid":"INV03091474","duedate":"4/20/2017","total":"800.00","currency":{"value":"1","text":"USA"}}}

    /**
     * If this entry point is used, the reduce function is invoked one time for
     * each key and list of values provided..
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
    function reduce(context) {

      context.write({
        key: 1,
        value: context.values
      });
    }

    /**
     * If this entry point is used, the reduce function is invoked one time for
     * each key and list of values provided..
     *
     * @param {Object} context
     * @param {boolean} context.isRestarted - Indicates whether the current invocation of the represents a restart.
     * @param {number} context.concurrency - The maximum concurrency number when running the map/reduce script.
     * @param {Date} context.datecreated - The time and day when the script began running.
     * @param {number} context.seconds - The total number of seconds that elapsed during the processing of the script.
     * @param {number} context.usage - TThe total number of usage units consumed during the processing of the script.
     * @param {number} context.yields - The total number of yields that occurred during the processing of the script.
     * @param {Object} context.inputSummary - Object that contains data about the input stage.
     * @param {Object} context.mapSummary - Object that contains data about the map stage.
     * @param {Object} context.reduceSummary - Object that contains data about the reduce stage.
     * @param {Iterator} context.ouput - This param contains a "iterator().each(parameters)" function
     *
     * @since 2016.1
     */
    function summarize(context) {
      var contador = 0;
      log.debug("summarize", "empieza el iterator");
      ObtenerParametrosYFeatures();
      ObtenerDatosSubsidiaria();
      try {
        log.debug("summarize", "empieza el iterator");

        context.output.iterator().each(function(key, value) {
          contador += 1;
          return true;
        });
        GLOBAL_LABELS = getGlobalLabels();
        if (contador == 0) {
          NoData();
        } else {
          GenerarExcel(context.output.iterator());
          GenerarXml(context.output.iterator());
        }
      } catch (error) {
        log.error("error", error);
      }
    }

    function getGlobalLabels() {
      var labels = {
        "titulo": {
          "es": 'FORMULARIO 1012: INFORMACION DE LAS DECLARACIONES TRIBUTARIAS, ACCIONES Y APORTES E INVERSIONES EN BONOS, CERTIFICADOS, TITULOS Y DEMAS INVERSIONES TRIBUTARIAS',
          "pt": 'FORMULARIO 1012: INFORMAÇÕES SOBRE DECLARAÇÕES FISCAIS, AÇÕES E CONTRIBUIÇÕES E INVESTIMENTOS EM OBRIGAÇÕES, CERTIFICADOS, TITULOS E OUTROS INVESTIMENTOS FISCAIS',
          "en": 'FORM 1012: INFORMATION ON TAX DECLARATIONS, SHARES AND CONTRIBUTIONS AND INVESTMENTS IN BONDS, CERTIFICATES, SECURITIES AND OTHER TAX INVESTMENTS'
        },
        "razonSocial": {
          "es": 'Razon Social',
          "pt": 'Razao social',
          "en": 'Company name'
        },
        "taxNumber": {
          "es": 'Numero de Identificacion Tributaria',
          "pt": 'Numero de Identificacao Fiscal',
          "en": 'Tax Number'
        },
        "periodo": {
          "es": 'Periodo',
          "pt": 'Periodo',
          "en": 'Period'
        },
        "primerApellido": {
          "es": '1er Apellido',
          "pt": '1º Sobrenome',
          "en": '1st Last Name'
        },
        "segApellido": {
          "es": '2do Apellido',
          "pt": '2º Sobrenome',
          "en": '2nd Last Name'
        },
        "primerNombre": {
          "es": '1er Nombre',
          "pt": '1º nome',
          "en": '1st Name'
        },
        "segNombre": {
          "es": '2do Nombre',
          "pt": '2º nome',
          "en": '2nd Name'
        },
        "pais": {
          "es": 'Pais',
          "pt": 'Pais',
          "en": 'Country'
        },
        "valorAl": {
          "es": 'Valor al 31 - 12',
          "pt": 'Valor em 31 - 12',
          "en": 'Value at 31 - 12'
        },
        "hasta": {
          "es": 'hasta',
          "pt": 'até',
          "en": 'until'
        },
        'noData': {
          "es": 'No existe informacion para los criterios seleccionados',
          "pt": 'Não há informações para os critérios selecionados',
          "en": 'There is no information for the selected criteria'
        },
        'libroContable': {
          "es": 'Libro Contable',
          "pt": 'Livro de Contabilidade',
          "en": 'Accounting Book'
        },
        "origin": {
          "es": 'Origen',
          "pt": 'Origem',
          "en": 'Origin'
        },
        "date": {
          "es": 'Fecha',
          "pt": 'Data',
          "en": 'Date'
        },
        "time": {
          "es": 'Hora',
          "pt": 'Hora',
          "en": 'Time'
        }
      }

      return labels;
    }

    function ObtieneBank() {
      // Control de Memoria
      var intDMaxReg = 1000;
      var intDMinReg = 0;
      var arrAuxiliar = new Array();

      // Exedio las unidades
      var Dusager = false;
      var DbolStop = false;
      var usageRemaining = objContext.getRemainingUsage();

      var _cont = 0;

      var savedSearch = search.load({
        id: 'customsearch_lmry_co_bank'
      });

      var searchresult = savedSearch.run();
      var objResult = searchresult.getRange(intDMinReg, intDMaxReg);
      if (objResult != null) {
        var intLength = objResult.length;

        for (var i = 0; i < intLength; i++) {

          columns = objResult[i].columns;
          arrAuxiliar = new Array();

          //O. NOMBRE
          if (objResult[i].getValue(columns[0]) != null)
            arrAuxiliar[0] = validarAcentos(objResult[i].getValue(columns[0]));
          else
            arrAuxiliar[0] = '';
          //1. ID
          if (objResult[i].getValue(columns[1]) != null)
            arrAuxiliar[1] = objResult[i].getValue(columns[1]);
          else
            arrAuxiliar[1] = '';
          //2. NOMBRE CORTO
          if (objResult[i].getValue(columns[2]) != null)
            arrAuxiliar[2] = validarAcentos(objResult[i].getValue(columns[2]));
          else
            arrAuxiliar[2] = '';
          //3. CLAVE
          if (objResult[i].getValue(columns[3]) != null)
            arrAuxiliar[3] = objResult[i].getValue(columns[3]);
          else
            arrAuxiliar[3] = '';
          //4. PAIS
          if (objResult[i].getValue(columns[4]) != null)
            arrAuxiliar[4] = objResult[i].getValue(columns[4]);
          else
            arrAuxiliar[4] = '';
          //5. CO TDOC
          if (objResult[i].getValue(columns[5]) != null)
            arrAuxiliar[5] = objResult[i].getValue(columns[5]);
          else
            arrAuxiliar[5] = '';
          //6. CODIGO PAIS
          if (objResult[i].getValue(columns[6]) != null)
            arrAuxiliar[6] = objResult[i].getValue(columns[6]);
          else
            arrAuxiliar[6] = '';
          //7. DV
          if (objResult[i].getValue(columns[7]) != null)
            arrAuxiliar[7] = objResult[i].getValue(columns[7]);
          else
            arrAuxiliar[7] = '';
          //8. NID
          if (objResult[i].getValue(columns[8]) != null)
            arrAuxiliar[8] = objResult[i].getValue(columns[8]);
          else
            arrAuxiliar[8] = '';
          //9. RAZON SOCIAL
          if (objResult[i].getValue(columns[9]) != null)
            arrAuxiliar[9] = validarAcentos(objResult[i].getValue(columns[9]));
          else
            arrAuxiliar[9] = '';

          //nlapiLogExecution('DEBUG', 'arrAuxiliar-> ',arrAuxiliar[0]+','+arrAuxiliar[1]+','+arrAuxiliar[2]+','+arrAuxiliar[3]);
          ArrBank[_cont] = arrAuxiliar;
          _cont++;
        }
        intDMinReg = intDMaxReg;
        intDMaxReg += 1000;
        if (intLength < 1000) {
          DbolStop = true;
        }
      } else {
        DbolStop = true;
      }
    }

    function ObtenerParametrosYFeatures() {

      paramSubsidiaria = objContext.getParameter({
        name: 'custscript_lmry_subsid_form101_mprd'
      });
      paramMultibook = objContext.getParameter({
        name: 'custscript_lmry_multi_form1012_mprd'
      });
      paramPeriodo = objContext.getParameter({
        name: 'custscript_lmry_period_form1012anu_m_v71'
      });
      paramIdLog = objContext.getParameter({
        name: 'custscript_lmry_idlog_form1012anu_m_v71'
      });
      paramIdReport = objContext.getParameter({
        name: 'custscript_lmry_idrepo_form1012anu_m_v71'
      });
      paramCont = '0';

      paramIdFeatureByVersion = objContext.getParameter({
        name: 'custscript_lmry_byvers_form1012anu_m_v71'
      });
      paramConcepto = objContext.getParameter({
        name: 'custscript_lmry_concep_form1012anu_m_v71'
      });

      if (paramCont == null) {
        paramCont = 0;
      }

      isSubsidiariaFeature = runtime.isFeatureInEffect({
        feature: 'SUBSIDIARIES'
      });
      isMultibookFeature = runtime.isFeatureInEffect({
        feature: 'MULTIBOOK'
      });
      log.debug('parametros', paramSubsidiaria + ' - ' + paramMultibook + ' - ' + paramPeriodo + ' - ' + paramIdReport + ' - ' + paramCont + ' - ' + paramIdFeatureByVersion + ' - ' + paramConcepto);

      periodEndDate = new Date(paramPeriodo, 11, 31);

      periodEndDate = format.format({
        value: periodEndDate,
        type: format.Type.DATE
      });

      periodEndDateFormat = format.parse({
        value: periodEndDate,
        type: format.Type.DATE
      });

      var MM = periodEndDateFormat.getMonth() + 1;
      var AAAA = periodEndDateFormat.getFullYear();
      var DD = periodEndDateFormat.getDate();

      var periodAux = DD + '/' + MM + '/' + AAAA;

      var auxiliar = periodAux.split('/');

      if (auxiliar[0].length == 1) {
        auxiliar[0] = '0' + auxiliar[0];
      }
      if (auxiliar[1].length == 1) {
        auxiliar[1] = '0' + auxiliar[1];
      }

      periodEndDateFormat = auxiliar[0] + '/' + auxiliar[1] + '/' + auxiliar[2];

      periodName = paramPeriodo;

      if (isMultibookFeature) {
        var multibook = search.lookupFields({
          type: search.Type.ACCOUNTING_BOOK,
          id: paramMultibook,
          columns: ['name']
        });

        multibookName = multibook.name;
      }

      if (paramIdReport != null) {
        var report = search.lookupFields({
          type: 'customrecord_lmry_co_features',
          id: paramIdReport,
          columns: ['name']
        });
        reportName = report.name;
      }
      log.debug("reportName", reportName);

    }

    function ObtenerDatosSubsidiaria() {

      if (isSubsidiariaFeature) {
        if (paramSubsidiaria != '' && paramSubsidiaria != null) {
          var subsidiaryField = search.lookupFields({
            type: search.Type.SUBSIDIARY,
            id: paramSubsidiaria,
            columns: ['taxidnum', 'legalname']
          });

          companyName = subsidiaryField.legalname;
          companyRuc = subsidiaryField.taxidnum;
        } else {
          log.debug('No existe id de subsidiaria.');
        }

      } else {
        var pageConfig = config.load({
          type: config.Type.COMPANY_INFORMATION
        });

        companyRuc = pageConfig.getFieldValue('employerid');
        companyName = pageConfig.getFieldValue('legalname');
      }
      companyRuc = companyRuc.replace(' ', '');
      companyName = validarAcentos(companyName);

    }

    function validarAcentos(s) {
      var AccChars = "ŠŽšžŸÀÁÂÃÄÅÇÈÉÊËÌÍÎÏÐÒÓÔÕÖÙÚÛÜÝàáâãäåçèéêëìíîïðòóôõöùúûüýÿ°–—ªº·";
      var RegChars = "SZszYAAAAAACEEEEIIIIDOOOOOUUUUYaaaaaaceeeeiiiidooooouuuuyyo--ao.";

      s = s.toString();
      for (var c = 0; c < s.length; c++) {
        for (var special = 0; special < AccChars.length; special++) {
          if (s.charAt(c) == AccChars.charAt(special)) {
            s = s.substring(0, c) + RegChars.charAt(special) + s.substring(c + 1, s.length);
          }
        }
      }
      return s;
    }

    function ObtenerCuentas() {
      var savedSearch = search.create({
        type: 'account',
        filters: [
          ["custrecord_lmry_co_puc_formatgy", "is", "10"],
          "AND",
          ["custrecord_lmry_co_puc_concept.custrecord_lmry_co_puc_formatid_c", "is", "1012"]
        ],
        columns: [
          search.createColumn({
            name: 'internalid'
          }),
          search.createColumn({
            name: 'formulatext',
            formula: 'SUBSTR({custrecord_lmry_co_puc_concept}, 1,4)'
          }),
          search.createColumn({
            name: 'formulatext',
            formula: '{custrecord_lmry_bank_name}'
          }),
          search.createColumn({
            name: "number"
          }),
          search.createColumn({
            name: "type"
          })
        ]
      });

      var accountJson = {};
      var pagedData = savedSearch.runPaged({
        pageSize: 1000
      });
      var page;
      var aux = [];
      pagedData.pageRanges.forEach(function(pageRange) {
        page = pagedData.fetch({
          index: pageRange.index
        });
        page.data.forEach(function(result) {
          columns = result.columns;
          aux.push(result.getValue(columns[1]));
          aux.push(result.getValue(columns[2]));
          aux.push(result.getValue(columns[3]));
          aux.push(result.getValue(columns[4]));
          accountJson[result.getValue(columns[0])] = aux;
          aux = [];
        })
      });

      return accountJson;
    }

    function ObtenerInversiones(accountJson) {
      var json_final = {}
      var savedSearch = search.load({
        /*LatamReady - CO Form 1012 Tax Declaration V7.1*/
        id: 'customsearch_lmry_co_form1012_v71'
      });
      log.debug("valor de accountJson", accountJson);
      log.debug("key de accountJson", Object.keys(accountJson));

      if (paramPeriodo) {
        var fechFinFilter = search.createFilter({
          name: 'trandate',
          operator: search.Operator.ONORBEFORE,
          values: [periodEndDate]
        });
        savedSearch.filters.push(fechFinFilter);
      }

      if (isSubsidiariaFeature) {
        var subsidiaryFilter = search.createFilter({
          name: 'subsidiary',
          operator: search.Operator.IS,
          values: [paramSubsidiaria]
        });
        savedSearch.filters.push(subsidiaryFilter);
      }

      if (isMultibookFeature) {
        var accountsIdArray = Object.keys(accountJson);
        log.debug('accountsIdArray', accountsIdArray);

        var accountFilter = search.createFilter({
          name: 'account',
          join: 'accountingtransaction',
          operator: search.Operator.ANYOF,
          values: accountsIdArray
        });
        savedSearch.filters.push(accountFilter);

        var multibookFilter = search.createFilter({
          name: 'accountingbook',
          join: 'accountingtransaction',
          operator: search.Operator.IS,
          values: [paramMultibook]
        });
        savedSearch.filters.push(multibookFilter);

        var column0 = search.createColumn({
          name: 'account',
          join: 'accountingtransaction',
          summary: "GROUP"
        });

        savedSearch.columns[0] = column0;

        var column1 = search.createColumn({
          name: 'formulacurrency',
          formula: "NVL({accountingtransaction.debitamount},0) - NVL({accountingtransaction.creditamount},0)",
          summary: "SUM"
        });
        savedSearch.columns.push(column1);

      } else {
        var accountsIdArray = Object.keys(accountJson);
        log.debug('accountsIdArray', accountsIdArray);
        var accountFilter = search.createFilter({
          name: 'account',
          operator: search.Operator.ANYOF,
          values: accountsIdArray
        });
        savedSearch.filters.push(accountFilter);

        var column1 = search.createColumn({
          name: 'formulacurrency',
          formula: "NVL({debitamount},0) - NVL({creditamount},0)",
          summary: 'SUM'
        });
        savedSearch.columns.push(column1);
      }

      var pagedData = savedSearch.runPaged({
        pageSize: 1000
      });

      var page;
      log.debug("tamano pagedData", pagedData);

      pagedData.pageRanges.forEach(function(pageRange) {
        page = pagedData.fetch({
          index: pageRange.index
        });
        log.debug("index", pageRange.index);
        log.debug("page", page);

        page.data.forEach(function(result) {
          log.debug("valor result", result);
          var columns = result.columns;
          //log.debug("columnas",columns);
          //log.debug("valor de id:",result.getValue(columns[0]));
          var arrAuxiliar = new Array();
          var internalid = result.getValue(columns[0]);
          var amount = result.getValue(columns[1]);

          //0.- concepto
          if (accountJson[internalid][0] != '- None -')
            arrAuxiliar[0] = accountJson[internalid][0];
          else
            arrAuxiliar[0] = '';
          //1 - 9 .- Campos Latam Bank
          if (accountJson[internalid][1] != '- None -') {
            var nombreBanco = validarAcentos(accountJson[internalid][1]);
            if (ArrBank.length > 0) {
              for (var ccBank = 0; ccBank < ArrBank.length; ccBank++) {
                if (ArrBank[ccBank][0] == nombreBanco) {
                  arrAuxiliar[1] = ArrBank[ccBank][5];
                  arrAuxiliar[2] = RetornaNumero(ArrBank[ccBank][8]);
                  arrAuxiliar[3] = RecortarCaracteres(ArrBank[ccBank][7], 1);
                  arrAuxiliar[4] = '';
                  arrAuxiliar[5] = '';
                  arrAuxiliar[6] = '';
                  arrAuxiliar[7] = '';
                  arrAuxiliar[8] = ArrBank[ccBank][9];
                  arrAuxiliar[9] = ArrBank[ccBank][6];
                }
              }
            } else {
              arrAuxiliar[1] = '';
              arrAuxiliar[2] = '';
              arrAuxiliar[3] = '';
              arrAuxiliar[4] = '';
              arrAuxiliar[5] = '';
              arrAuxiliar[6] = '';
              arrAuxiliar[7] = '';
              arrAuxiliar[8] = '';
              arrAuxiliar[9] = '';
            }
          } else {
            arrAuxiliar[1] = '';
            arrAuxiliar[2] = '';
            arrAuxiliar[3] = '';
            arrAuxiliar[4] = '';
            arrAuxiliar[5] = '';
            arrAuxiliar[6] = '';
            arrAuxiliar[7] = '';
            arrAuxiliar[8] = '';
            arrAuxiliar[9] = '';
          }
          //10. VALOR AL 31-12
          if (amount != null)
            arrAuxiliar[10] = Math.abs(redondear(amount));
          else
            arrAuxiliar[10] = 0;

          //11 account number
          if (accountJson[internalid][2] != null)
            arrAuxiliar[11] = accountJson[internalid][2];
          else
            arrAuxiliar[11] = '';

          //12 account type
          if (accountJson[internalid][3] != null)
            arrAuxiliar[12] = accountJson[internalid][3];
          else
            arrAuxiliar[12] = '';

          var key_final = arrAuxiliar.slice(0, 9).join("||");
          if (arrAuxiliar[10] != 0) {
            if (json_final[key_final] != undefined) {
              json_final[key_final][10] += arrAuxiliar[10];
              json_final[key_final][10] = redondear(json_final[key_final][10]);
            } else {
              json_final[key_final] = arrAuxiliar;
            }
          }

        });
      });

      return json_final;
    }

    function completar_cero(long, valor) {

      if ((('' + valor).length) <= long) {
        if (long != ('' + valor).length) {
          for (var i = (('' + valor).length); i < long; i++) {
            valor = '0' + valor;
          }
        } else {
          return valor;
        }
        return valor;
      } else {
        valor = valor.substring(0, long);
        return valor;
      }

    }

    function Name_File() {
      var name = '';
      name = 'Dmuisca_' + completar_cero(2, paramConcepto) + '01012' + '71' + paramPeriodo + completar_cero(8, numeroEnvio);

      return name;
    }

    function SaveFile(extension) {
      var folderId = objContext.getParameter({
        name: 'custscript_lmry_file_cabinet_rg_co'
      });

      // Almacena en la carpeta de Archivos Generados
      if (folderId != '' && folderId != null) {
        // Extension del archivo
        var excel = true;
        var fileName = Name_File() + extension;

        // Crea el archivo
        var ventasXPagarFile;

        if (extension == '.xls') {
          ventasXPagarFile = file.create({
            name: fileName,
            fileType: file.Type.EXCEL,
            contents: strExcelInversiones,
            folder: folderId
          });

        } else {
          ventasXPagarFile = file.create({
            name: fileName,
            fileType: file.Type.PLAINTEXT,
            contents: strXmlInversiones,
            encoding: file.Encoding.UTF8,
            folder: folderId
          });
          excel = false;
        }

        var fileId = ventasXPagarFile.save();

        ventasXPagarFile = file.load({
          id: fileId
        });

        var getURL = objContext.getParameter({
          name: 'custscript_lmry_netsuite_location'
        });

        var fileUrl = '';

        if (getURL != '') {
          fileUrl += 'https://' + getURL;
        }

        fileUrl += ventasXPagarFile.url;

        if (fileId) {
          //log.debug("valor del runtime",runtime.getCurrentUser());
          var usuario = runtime.getCurrentUser();
          //log.debug("id del usuario",usuario.id);
          var employee = search.lookupFields({
            type: search.Type.EMPLOYEE,
            id: usuario.id,
            columns: ['firstname', 'lastname']
          });
          var usuarioName = employee.firstname + ' ' + employee.lastname;

          if (excel || generarXml) {
            var recordLog = record.create({
              type: 'customrecord_lmry_co_rpt_generator_log'
            });
          } else {
            var recordLog = record.load({
              type: 'customrecord_lmry_co_rpt_generator_log',
              id: paramIdLog
            });
          }

          //Nombre de Archivo
          recordLog.setValue({
            fieldId: 'custrecord_lmry_co_rg_name',
            value: fileName
          });

          //Url de Archivo
          recordLog.setValue({
            fieldId: 'custrecord_lmry_co_rg_url_file',
            value: fileUrl
          });

          //Nombre de Reporte
          recordLog.setValue({
            fieldId: 'custrecord_lmry_co_rg_transaction',
            value: reportName
          });

          //Nombre de Subsidiaria
          recordLog.setValue({
            fieldId: 'custrecord_lmry_co_rg_subsidiary',
            value: companyName
          });

          //Periodo
          recordLog.setValue({
            fieldId: 'custrecord_lmry_co_rg_postingperiod',
            value: periodName
          });

          if (isMultibookFeature) {
            //Multibook
            recordLog.setValue({
              fieldId: 'custrecord_lmry_co_rg_multibook',
              value: multibookName
            });
          }

          //Creado Por
          recordLog.setValue({
            fieldId: 'custrecord_lmry_co_rg_employee',
            value: usuarioName
          });

          recordLog.save();
          libreria.sendrptuser(reportName, 3, fileName);
        }
      } else {
        log.debug({
          title: 'Creacion de File:',
          details: 'No existe el folder'
        });
      }
    }

    function obtenerNumeroEnvio() {
      var numeroLote = 1;
      log.debug('idfeatyre', paramIdFeatureByVersion);

      var savedSearch = search.create({
        type: 'customrecord_lmry_co_lote_rpt_magnetic',
        filters: [
          search.createFilter({
            name: 'internalid',
            join: 'custrecord_lmry_co_id_magnetic_rpt',
            operator: search.Operator.IS,
            values: [paramIdFeatureByVersion]
          }),

          search.createFilter({
            name: 'internalid',
            join: 'custrecord_lmry_co_subsidiary',
            operator: search.Operator.IS,
            values: [paramSubsidiaria]
          }),

          search.createFilter({
            name: 'custrecord_lmry_co_year_issue',
            operator: search.Operator.IS,
            values: [paramPeriodo]
          })
        ],
        columns: ['internalid', 'custrecord_lmry_co_lote']
      });

      var objResult = savedSearch.run().getRange(0, 1000);

      if (objResult == null || objResult.length == 0) {

        var loteXRptMgnRecord = record.create({
          type: 'customrecord_lmry_co_lote_rpt_magnetic'
        });

        loteXRptMgnRecord.setValue({
          fieldId: 'custrecord_lmry_co_id_magnetic_rpt',
          value: paramIdFeatureByVersion
        });
        loteXRptMgnRecord.setValue({
          fieldId: 'custrecord_lmry_co_year_issue',
          value: paramPeriodo
        });
        loteXRptMgnRecord.setValue({
          fieldId: 'custrecord_lmry_co_lote',
          value: numeroLote
        });
        loteXRptMgnRecord.setValue({
          fieldId: 'custrecord_lmry_co_subsidiary',
          value: paramSubsidiaria
        });

      } else {
        var columns = objResult[0].columns;
        var internalId = objResult[0].getValue(columns[0]);
        numeroLote = Number(objResult[0].getValue(columns[1])) + 1;
        var loteXRptMgnRecord = record.load({
          type: 'customrecord_lmry_co_lote_rpt_magnetic',
          id: internalId
        });

        loteXRptMgnRecord.setValue({
          fieldId: 'custrecord_lmry_co_lote',
          value: numeroLote
        });

      }
      loteXRptMgnRecord.save();

      return numeroLote;
    }

    function NoData() {
      var usuario = runtime.getCurrentUser();

      var employee = search.lookupFields({
        type: search.Type.EMPLOYEE,
        id: usuario.id,
        columns: ['firstname', 'lastname']
      });
      var usuarioName = employee.firstname + ' ' + employee.lastname;

      if (Number(paramCont) > 1) {
        var generatorLog = record.create({
          type: 'customrecord_lmry_co_rpt_generator_log'
        });
      } else {
        var generatorLog = record.load({
          type: 'customrecord_lmry_co_rpt_generator_log',
          id: paramIdLog
        });
      }

      //Nombre de Archivo
      generatorLog.setValue({
        fieldId: 'custrecord_lmry_co_rg_name',
        value: GLOBAL_LABELS['noData'][language]
      });
      //Nombre de Reporte
      generatorLog.setValue({
        fieldId: 'custrecord_lmry_co_rg_transaction',
        value: reportName
      });
      //Nombre de Subsidiaria
      generatorLog.setValue({
        fieldId: 'custrecord_lmry_co_rg_subsidiary',
        value: companyName
      });
      //Periodo
      generatorLog.setValue({
        fieldId: 'custrecord_lmry_co_rg_postingperiod',
        value: periodName
      });

      if (isMultibookFeature) {
        //Multibook
        generatorLog.setValue({
          fieldId: 'custrecord_lmry_co_rg_multibook',
          value: multibookName
        });
      }
      //Creado Por
      generatorLog.setValue({
        fieldId: 'custrecord_lmry_co_rg_employee',
        value: usuarioName
      });

      var recordId = generatorLog.save();
    }
    /* ********************************************************************
     * Generacion de estructura excel y XML
     * ********************************************************************/
    function GenerarExcel(context) {
      var xlsString = '';

      //PDF Normalization
      var todays = parseDateTo(new Date(), "DATE");
      var currentTime = getTimeHardcoded(parseDateTo(new Date(), "DATETIME"));

      //cabecera de excel
      xlsString = '<?xml version="1.0" encoding="UTF-8" ?><?mso-application progid="Excel.Sheet"?>';
      xlsString += '<Workbook xmlns="urn:schemas-microsoft-com:office:spreadsheet" ';
      xlsString += 'xmlns:o="urn:schemas-microsoft-com:office:office" ';
      xlsString += 'xmlns:x="urn:schemas-microsoft-com:office:excel" ';
      xlsString += 'xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet" ';
      xlsString += 'xmlns:html="http://www.w3.org/TR/REC-html40">';
      xlsString += '<Styles>';
      xlsString += '<Style ss:ID="s21"><Font ss:Bold="1" ss:Size="12" /><Alignment ss:Horizontal="Center" ss:Vertical="Bottom"/></Style>';
      xlsString += '<Style ss:ID="s22"><Font ss:Bold="1"/><Alignment ss:Vertical="Bottom"/></Style>';
      xlsString += '<Style ss:ID="s23"><Font ss:Bold="1"/><Alignment ss:Vertical="Bottom"/><NumberFormat ss:Format="_(* ###0_);_(* \(###0\);_(@_)"/></Style>';
      xlsString += '<Style ss:ID="s24"><NumberFormat ss:Format="_(* ###0_);_(* \(###0\);_(@_)"/></Style>';
      xlsString += '</Styles><Worksheet ss:Name="Sheet1">';


      xlsString += '<Table>';
      xlsString += '<Column ss:AutoFitWidth="0" ss:Width="100"/>';
      xlsString += '<Column ss:AutoFitWidth="0" ss:Width="100"/>';
      xlsString += '<Column ss:AutoFitWidth="0" ss:Width="100"/>';
      xlsString += '<Column ss:AutoFitWidth="0" ss:Width="150"/>';
      xlsString += '<Column ss:AutoFitWidth="0" ss:Width="150"/>';
      xlsString += '<Column ss:AutoFitWidth="0" ss:Width="150"/>';
      xlsString += '<Column ss:AutoFitWidth="0" ss:Width="150"/>';
      xlsString += '<Column ss:AutoFitWidth="0" ss:Width="150"/>';
      xlsString += '<Column ss:AutoFitWidth="0" ss:Width="150"/>';
      xlsString += '<Column ss:AutoFitWidth="0" ss:Width="150"/>';
      xlsString += '<Column ss:AutoFitWidth="0" ss:Width="150"/>';
      xlsString += '<Column ss:AutoFitWidth="0" ss:Width="150"/>';
      xlsString += '<Column ss:AutoFitWidth="0" ss:Width="150"/>';

      //Cabecera
      xlsString += '<Row>';
      xlsString += '<Cell></Cell>';
      xlsString += '<Cell></Cell>';
      xlsString += '<Cell></Cell>';
      xlsString += '<Cell></Cell>';
      xlsString += '<Cell></Cell>';
      xlsString += '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['titulo'][language] + '  </Data></Cell>';
      xlsString += '</Row>';
      xlsString += '<Row></Row>';
      xlsString += '<Row>';
      xlsString += '<Cell></Cell>';
      xlsString += '<Cell></Cell>';
      xlsString += '<Cell></Cell>';
      xlsString += '<Cell><Data ss:Type="String">' + GLOBAL_LABELS['razonSocial'][language] + ': ' + companyName + '</Data></Cell>';
      xlsString += '</Row>';
      xlsString += '<Row>';
      xlsString += '<Cell></Cell>';
      xlsString += '<Cell></Cell>';
      xlsString += '<Cell></Cell>';
      xlsString += '<Cell><Data ss:Type="String">' + GLOBAL_LABELS['taxNumber'][language] + ': ' + companyRuc + '</Data></Cell>';
      xlsString += '</Row>';
      xlsString += '<Row>';
      xlsString += '<Cell></Cell>';
      xlsString += '<Cell></Cell>';
      xlsString += '<Cell></Cell>';
      xlsString += '<Cell><Data ss:Type="String">' + GLOBAL_LABELS['periodo'][language] + ': ' + GLOBAL_LABELS['hasta'][language] + ' ' + periodEndDateFormat + '</Data></Cell>';
      xlsString += '</Row>';
      if (isMultibookFeature) {
        xlsString += '<Row>';
        xlsString += '<Cell></Cell>';
        xlsString += '<Cell></Cell>';
        xlsString += '<Cell></Cell>';
        xlsString += '<Cell><Data ss:Type="String">' + GLOBAL_LABELS['libroContable'][language] + ': ' + multibookName + '</Data></Cell>';
        xlsString += '</Row>';
      }

      // PDF Normalized

      xlsString += '<Row>';
      xlsString += '<Cell></Cell>';
      xlsString += '<Cell></Cell>';
      xlsString += '<Cell></Cell>';
      xlsString += '<Cell><Data ss:Type="String">' + GLOBAL_LABELS['origin'][language] + ': Netsuite' + '</Data></Cell>';
      xlsString += '</Row>';
      xlsString += '<Row>';
      xlsString += '<Cell></Cell>';
      xlsString += '<Cell></Cell>';
      xlsString += '<Cell></Cell>';
      xlsString += '<Cell><Data ss:Type="String">' + GLOBAL_LABELS['date'][language] + ': ' + todays + '</Data></Cell>';
      xlsString += '</Row>';
      xlsString += '<Row>';
      xlsString += '<Cell></Cell>';
      xlsString += '<Cell></Cell>';
      xlsString += '<Cell></Cell>';
      xlsString += '<Cell><Data ss:Type="String">' + GLOBAL_LABELS['time'][language] + ': ' + currentTime + '</Data></Cell>';
      xlsString += '</Row>';

      // END PDF Normalized

      xlsString += '<Row></Row>';
      xlsString += '<Row></Row>';
      xlsString += '<Row>' +
        '<Cell ss:StyleID="s21"><Data ss:Type="String"> CPT </Data></Cell>' +
        '<Cell ss:StyleID="s21"><Data ss:Type="String"> TDOC </Data></Cell>' +
        '<Cell ss:StyleID="s21"><Data ss:Type="String"> NID </Data></Cell>' +
        '<Cell ss:StyleID="s21"><Data ss:Type="String"> D.V. </Data></Cell>' +
        '<Cell ss:StyleID="s21"><Data ss:Type="String">' + GLOBAL_LABELS['primerApellido'][language] + '</Data></Cell>' +
        '<Cell ss:StyleID="s21"><Data ss:Type="String">' + GLOBAL_LABELS['segApellido'][language] + '</Data></Cell>' +
        '<Cell ss:StyleID="s21"><Data ss:Type="String">' + GLOBAL_LABELS['primerNombre'][language] + '</Data></Cell>' +
        '<Cell ss:StyleID="s21"><Data ss:Type="String">' + GLOBAL_LABELS['segNombre'][language] + '</Data></Cell>' +
        '<Cell ss:StyleID="s21"><Data ss:Type="String">' + GLOBAL_LABELS['razonSocial'][language] + '</Data></Cell>' +
        '<Cell ss:StyleID="s21"><Data ss:Type="String">' + GLOBAL_LABELS['pais'][language] + '</Data></Cell>' +
        '<Cell ss:StyleID="s21"><Data ss:Type="String">' + GLOBAL_LABELS['valorAl'][language] + '</Data></Cell>' +
        '</Row>';

      //creacion de reporte xls
      context.each(function(key, value) {

        var arreglo0 = JSON.parse(value);
        var objResult = JSON.parse(arreglo0[0]);
        //objResult = objResult[0];
        xlsString += '<Row>';
        //0. CPT
        if (objResult[0] != '' && objResult[0] != null && objResult[0] != '- None -') {
          xlsString += '<Cell><Data ss:Type="String">' + objResult[0] + '</Data></Cell>';
        } else {
          xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
        }
        //1. TDOC
        if (objResult[1] != '' && objResult[1] != null && objResult[1] != '- None -') {
          xlsString += '<Cell><Data ss:Type="String">' + objResult[1] + '</Data></Cell>';
        } else {
          xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
        }
        //2. NID
        if (objResult[2] != '' && objResult[2] != null && objResult[2] != '- None -') {
          xlsString += '<Cell><Data ss:Type="String">' + objResult[2] + '</Data></Cell>';
        } else {
          xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
        }
        //3. D.V.
        if (objResult[3] != '' && objResult[3] != null && objResult[3] != '- None -') {
          xlsString += '<Cell><Data ss:Type="String">' + objResult[3] + '</Data></Cell>';
        } else {
          xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
        }
        //4. 1ER APELL
        if (objResult[4] != '' && objResult[4] != null && objResult[4] != '- None -') {
          if (objResult[4].split(' ').length <= 2) {
            xlsString += '<Cell><Data ss:Type="String">' + objResult[4].split(' ')[0] + '</Data></Cell>';
          } else {
            xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
          }
        } else {
          xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
        }
        //5. 2DO APELL
        if (objResult[5] != '' && objResult[5] != null && objResult[5] != '- None -') {
          if (objResult[5].split(' ').length == 2) {
            xlsString += '<Cell><Data ss:Type="String">' + objResult[5].split(' ')[1] + '</Data></Cell>';
          } else {
            xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
          }
        } else {
          xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
        }
        //6. 1ER NOMBRE
        if (objResult[6] != '' && objResult[6] != null && objResult[6] != '- None -') {
          if (objResult[6].split(' ').length <= 2) {
            xlsString += '<Cell><Data ss:Type="String">' + objResult[6].split(' ')[0] + '</Data></Cell>';
          } else {
            xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
          }
        } else {
          xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
        }
        //7. 2DO NOMBRE
        if (objResult[7] != '' && objResult[7] != null && objResult[7] != '- None -') {
          if (objResult[7].split(' ').length == 2) {
            xlsString += '<Cell><Data ss:Type="String">' + objResult[7].split(' ')[1] + '</Data></Cell>';
          } else {
            xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
          }
        } else {
          xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
        }
        //8. RAZON SOCIAL
        if (objResult[8] != '' && objResult[8] != null && objResult[8] != '- None -') {
          xlsString += '<Cell><Data ss:Type="String">' + objResult[8] + '</Data></Cell>';
        } else {
          xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
        }
        //9. Pais
        if (objResult[9] != '' && objResult[9] != null && objResult[9] != '- None -') {
          xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">' + Number(objResult[9]).toFixed(0) + '</Data></Cell>';
        } else {
          xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number"></Data></Cell>';
        }
        //10. VALOR AL 31-12
        if (objResult[10] != '' && objResult[10] != null && objResult[10] != '- None -') {
          xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">' + Number(objResult[10]).toFixed(0) + '</Data></Cell>';
        } else {
          xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number"></Data></Cell>';
        }
        //xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">0</Data></Cell>';
        xlsString += '</Row>';
        // }
        return true;
      });
      //} //fin del quiebre por clase

      xlsString += '</Table></Worksheet></Workbook>';

      strExcelInversiones = encode.convert({
        string: xlsString,
        inputEncoding: encode.Encoding.UTF_8,
        outputEncoding: encode.Encoding.BASE_64
      });
      numeroEnvio = obtenerNumeroEnvio();

      SaveFile('.xls');
    }

    function GenerarXml(context) {
      strXmlInversiones = '';
      strXmlInversionesAux = '';
      var cantidadDatos = 0;


      var today = new Date();
      today = getTimeZoneDate(today);
      log.debug("Timezone_date", today);
      //"2021-04-29T18:31:00.000Z"
      var anio = today.getFullYear();
      //log.debug("anio",anio);
      var mes = completar_cero(2, today.getMonth() + 1);
      //log.debug("today.getMonth",today.getMonth());
      //log.debug("anio",mes);
      var day = completar_cero(2, today.getDate());
      //log.debug("today.getDay()",today.getDay());
      //log.debug("anio",day);
      var hour = completar_cero(2, today.getHours());
      //log.debug("today.getHours()",today.getHours());
      //log.debug("anio",hour);
      var min = completar_cero(2, today.getMinutes());
      //log.debug("today.getMinutes()",today.getMinutes());
      //log.debug("anio",min);
      var sec = completar_cero(2, today.getSeconds());
      //log.debug("today.getSeconds()",today.getSeconds());
      //log.debug("anio",sec);
      today = anio + '-' + mes + '-' + day + 'T' + hour + ':' + min + ':' + sec;
      valorTotal = 0;

      context.each(function(key, value) {
        var arreglo0 = JSON.parse(value);
        var objResult = JSON.parse(arreglo0[0]);

        //log.debug("valor original1",redondear(objResult[10]));
        //log.debug("valor original2",Number(objResult[10]));
        //log.debug("valor de objResult", Math.round(Number(redondear(objResult[10]))));
        var numero_sumar = Number(objResult[10]);
        var negativo = false;

        if (numero_sumar < 0) {
          numero_sumar = numero_sumar * -1;
          negativo = true;
        }
        var numero_sumar = Math.round(numero_sumar);

        if (negativo) {
          numero_sumar = numero_sumar * -1;
        }

        valorTotal += numero_sumar;
        log.debug("valor total suma", valorTotal);

        if (objResult[10] < 0) {
          objResult[10] = objResult[10] * (-1);
        }

        strXmlInversionesAux += '<dectri' + ' cpt="' + objResult[0] + '" tdoc="' + objResult[1] + '" nid="' + objResult[2] + '" dv="' + objResult[3];

        //SI el campo nombre esta vacio
        if (objResult[8] != '' && objResult[8] != null && objResult[8] != '- None -') {
          strXmlInversionesAux += '" raz="' + objResult[8];
        } else {
          strXmlInversionesAux += '" raz="';
        }

        if (objResult[6] && objResult[6].split(' ')[0]) {
          strXmlInversionesAux += '" nomb1="' + objResult[i][6].split(' ')[0];
        } else {
          strXmlInversionesAux += '" nomb1="';
        }

        if (objResult[7] && objResult[7].split(' ')[1]) {
          strXmlInversionesAux += '" nomb2="' + objResult[7].split(' ')[1];
        } else {
          strXmlInversionesAux += '" nomb2="';
        }

        if (objResult[4] && objResult[4].split(' ')[0]) {
          strXmlInversionesAux += '" apl1="' + objResult[4].split(' ')[0];
        } else {
          strXmlInversionesAux += '" apl1="';
        }

        if (objResult[5] && objResult[5].split(' ')[0]) {
          strXmlInversionesAux += '" apl2="' + objResult[5].split(' ')[1];
        } else {
          strXmlInversionesAux += '" apl2="';
        }

        strXmlInversionesAux += '" pais="' + objResult[9] + '" val="' + parseFloat(objResult[10]).toFixed(0);
        strXmlInversionesAux += '"/> \r\n';

        cantidadDatos++;
        return true;
      });

      if (valorTotal < 0) {
        valorTotal = valorTotal * (-1);
      }

      strXmlInversiones += '<?xml version="1.0" encoding="ISO-8859-1"?> \r\n';
      strXmlInversiones += '<mas xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"> \r\n';
      strXmlInversiones += '<Cab> \r\n';
      strXmlInversiones += '<Ano>' + paramPeriodo + '</Ano> \r\n';
      strXmlInversiones += '<CodCpt>' + paramConcepto + '</CodCpt> \r\n';
      strXmlInversiones += '<Formato>1012</Formato> \r\n';
      strXmlInversiones += '<Version>71</Version> \r\n';
      strXmlInversiones += '<NumEnvio>' + numeroEnvio + '</NumEnvio> \r\n';
      strXmlInversiones += '<FecEnvio>' + today + '</FecEnvio> \r\n';
      strXmlInversiones += '<FecInicial></FecInicial> \r\n';
      strXmlInversiones += '<FecFinal>' + paramPeriodo + '-12-31</FecFinal> \r\n';
      strXmlInversiones += '<ValorTotal>' + valorTotal + '</ValorTotal> \r\n';
      strXmlInversiones += '<CantReg>' + cantidadDatos + '</CantReg> \r\n';
      strXmlInversiones += '</Cab>\r\n';
      strXmlInversiones += strXmlInversionesAux;
      strXmlInversiones += '</mas> \r\n';

      SaveFile('.xml');
    }

    function RetornaNumero(nid) {
      if (nid != null && nid != '') {
        return nid.replace(/(\.|\-)/g, '');
      }
      return '';
    }

    function RecortarCaracteres(valor, numero) {
      if (valor != null && valor.length > numero) {
        return valor.substring(0, numero);
      }
      return valor;
    }

    function redondear(number) {
      return Math.round(Number(number) * 100) / 100;
    }

    function getTimeZoneDate(date) {
      var timeZone = runtime.getCurrentScript().getParameter("TIMEZONE");

      var timeZoneDateObject = date;

      if (timeZone) {
        timeZoneDateString = format.format({
          value: date,
          type: format.Type.DATETIME,
          timezone: timeZone
        });

        timeZoneDateObject = format.parse({
          value: timeZoneDateString,
          type: format.Type.DATE,
          timezone: timeZone
        });
      }

      return timeZoneDateObject;
    }

    function parseDateTo(trandate, type) {
        var $date = '';

        if (!trandate) return;

        // In Scheduled or Map/Reduce scripts the user timezone is not available
        var userObj = runtime.getCurrentUser();
        var userPrefTime = userObj.getPreference({ name: 'TIMEZONE' });

        $date = format.format({ value: trandate, type: format.Type[type], timezone: userPrefTime });

        return $date;
    }
  
    //** Function used to Get Current Time by only DAYTIME*/
    function getTimeHardcoded(datetime){

        if (!datetime) return;

        // This is provider by NetSuite Settings > User Preferences > Time Format
        var timeFormat = {
            "h:mm a": ":",
            "H:mm": ":",
            "h-mm a": "-",
            "H-mm": "-",
        }

        var userObj = runtime.getCurrentUser();
        var userPrefTimeFormat = userObj.getPreference({ name: 'TIMEFORMAT' });

        var separator = timeFormat[userPrefTimeFormat];

        var time = datetime.split(" ")[1];
        var ampm = datetime.split(" ")[2];

        var hours = time.split(separator)[0];
        var minutes = time.split(separator)[1];

        var time_ampm = hours + separator + minutes + " " + ampm;
        time = hours + separator + minutes;

        return  (ampm) ? time_ampm : time;
    }

    return {
      getInputData: getInputData,
      //map: map,
      reduce: reduce,
      summarize: summarize
    };

  });
