/* = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =\
||   This script for customer center (Time)                         ||
||                                                                  ||
||  File Name: LMRY_CO_RptLibroMayorBalanceDetallado_SCHDL_2.0.js   ||
||                                                                  ||
||  Version Date         Author          Remarks                    ||
||  2.0     Oct 12 2022  Jeferson Mejia  Use Script 2.0             ||
 \= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = */
/**
 * @NApiVersion 2.0
 * @NScriptType ScheduledScript
 * @NModuleScope Public
 */

define(["N/record", "N/runtime", "N/file", "N/encode", "N/search", "N/format", "N/log", "N/config", 'N/render'],

  function (recordModulo, runtime, fileModulo, encode, search, format, log, config, render) {

    var objContext = runtime.getCurrentScript();
    // Nombre del Reporte
    var namereport = "Reporte de Libro Mayor y Balance (Detallado)";
    var LMRY_script = 'LMRY CO Reportes Libro Mayor y Balance SCHDL (Detallado)';

    //Parametros
    var paramsubsidi = null;
    var paramperiodo = null;
    var paramidlog = null;
    var paramBucle = null;
    var paramAdjustment = null;
    var paramMultibook = null;
    var paramOchoDigitos = null;
    var error8digitos = '';

    //Control de Reporte
    var periodstartdate;
    var periodenddate;
    var antperiodenddate = null;
    var periodname;

    var xlsString = '';
    var ArrPeriodos = new Array();
    var flagEmpty = false;
    var arrAccountingContext = new Array();

    var ArrMontosAntes = new Array();
    var ArrMontosActual = new Array();
    var ArrMontosFinal = new Array();
    var ArrMontosAntesSpecific = new Array();
    var ArrMontosActualSpecific = new Array();
    var ArrCuentas = new Array();
    var calendarSubsi = null;
    var taxCalendarSubsi = null;
    // JSON
    var puc8DigDescript = new Object();



    //Cambio de Idioma
    var language = runtime.getCurrentScript().getParameter("LANGUAGE").substring(0, 2);
    var GLOBAL_LABELS = getGlobalLabels();

    //Datos Subsidiaria
    var companyruc = null;
    var companyname = null;

    var multibook_name = '';
    var AccountingPeriodsArray = new Array();
    var periodoAdjust = null;

    //Para el Special Accounting Period
    var periodstartdateSpecial;
    var periodenddateSpecial;
    var yearDateYY;
    var yearDateMM;
    var yearStartD;

    //PDF Normalization
    var todays = "";
    var currentTime = "";

    /*
     * 0 -> cuenta
     * 1 -> denominacion
     */
    var RecordName = 'customrecord_lmry_co_rpt_generator_log';
    var RecordTable = ['custrecord_lmry_co_rg_name',
      'custrecord_lmry_co_rg_postingperiod',
      'custrecord_lmry_co_rg_subsidiary',
      'custrecord_lmry_co_rg_url_file',
      'custrecord_lmry_co_rg_employee',
      'custrecord_lmry_co_rg_multibook',
    ];

    // Valida si es OneWorld
    var featureMultipCalendars = runtime.isFeatureInEffect({
      feature: 'MULTIPLECALENDARS'
    });
    var featuresubs = runtime.isFeatureInEffect({
      feature: "SUBSIDIARIES"
    });
    var feamultibook = runtime.isFeatureInEffect({
      feature: "MULTIBOOK"
    });
    var featurePeriodEnd = runtime.isFeatureInEffect({
      feature: "PERIODENDJOURNALENTRIES"
    });

    var featAccountingSpecial;
    var libraryRPT = '';

    var flagPDF = false;

    // Json para Cambio de Cuentas en Saldos Anteriores
    var jsonIndexCuentas = { get: function (key) { return this.data[key] }, set: function (key, value) { this.data[key] = value }, data: {} };

    function execute(scriptContext) {
      try {
        getLibraryRPT();
        ObtenerParametros();
        if (error8digitos) {
          return false;
        }
        ObtienePeriodoContable();
        CapturaInfoPeriodo(paramperiodo);
        if (paramOchoDigitos == 'T') {
          puc8digDescription();
        }
        if (feamultibook) {
          if (!ValidarPrimaryBook()) {
            var array_context = ObtieneAccountingContext();
          }
        }

        if (feamultibook) {
          ObtenerCuentas();
        }

        //! DESDE AQUI COMPLETAR
        periodoAdjust = obtenerPeriodoAdjustment();
        ArrMontosActual = ObtieneLibroMayor(true, false); //Agregar otro argumento para 262
        log.debug("[execute] Nro. lineas movimientos", ArrMontosActual.length);
        //agregar condicional para adjustment
        if (paramAdjustment == 'T' && periodoAdjust != null) {
          var ArrayAdjustment = ObtieneLibroMayor(true, true);
          log.debug("[execute] Nro. lineas movimientos (ajuste)", ArrayAdjustment.length);
          if (ArrayAdjustment.length != 0) {
            ArrMontosActual = ArrMontosActual.concat(ArrayAdjustment);
          }
        }

        if (feamultibook) {
          if (ArrCuentas.length != 0) {
            CambiarCuentasMultibook(ArrMontosActual);
          }

          // MOVIMIENTOS
          ArrMontosActualSpecific = ObtieneSpecificTransaction(true, 1);
          log.debug("[execute] Nro. lineas movimientos (transacciones especificas)", ArrMontosActualSpecific.length);
          if (ArrMontosActualSpecific.length != 0) {
            ArrMontosActual = ArrMontosActual.concat(ArrMontosActualSpecific);
          }
        } else {
          deleteAccountsWithoutPuc(ArrMontosActual);
        }

        ObtenerFechas(periodstartdate);

        if (antperiodenddate != null) {
          //SALDO ANTERIOR Y Adjust TRUE
          ArrMontosAntes = ObtieneLibroMayor(false, false);
          log.debug("[execute] Nro. lineas saldo anterior", ArrMontosAntes.length);
          // Saldo Inicial Restante para PUCs 4, 5, 6 y 7 (Cuentas de Resultados)(Cambio Card D0621)
          var ArrMontosAntesRestantes = ObtieneLMRestantesPUCs4567();
          log.debug("[execute] Nro. lineas saldo anterior restantes", ArrMontosAntesRestantes.length);

          if (ArrMontosAntesRestantes.length > 0) {
            ArrMontosAntes = ArrMontosAntes.concat(ArrMontosAntesRestantes);
          }

          if (featurePeriodEnd && feamultibook && (paramAdjustment == 'T')) {
            var arrSaldoAnteriorPEJ = getSaldoAnteriorPEJ(1);
            log.debug("[execute] Nro. lineas saldo anterior PEJ", arrSaldoAnteriorPEJ.length);
            ArrMontosAntes = ArrMontosAntes.concat(arrSaldoAnteriorPEJ);
            // Saldo Inicial Restante para PUCs 4, 5, 6 y 7 (Cuentas de Resultados)(Cambio Card D0621)
            var arrSaldoAnteriorPEJRestantes = getSaldoAnteriorPEJ(2);
            log.debug("[execute] Nro. lineas saldo anterior PEJ Restantes", arrSaldoAnteriorPEJRestantes.length);
            ArrMontosAntes = ArrMontosAntes.concat(arrSaldoAnteriorPEJRestantes);
          }

          if (feamultibook) {
            deleteAccountsWithoutPuc(ArrMontosAntes);
            // SALDO ANTERIOR
            ArrMontosAntesSpecific = ObtieneSpecificTransaction(false, 2);
            log.debug("[execute] Nro. lineas saldo anterior (transac. especificas)", ArrMontosAntesSpecific.length);
            if (ArrMontosAntesSpecific.length != 0) {
              ArrMontosAntes = ArrMontosAntes.concat(ArrMontosAntesSpecific);
            }
            // Saldo Inicial Restante para PUCs 4, 5, 6 y 7 (Cuentas de Resultados)(Cambio Card D0621)
            var ArrMontosAntesSpecificRestantes = ObtieneSpecificTransaction(false, 3);
            log.debug("[execute] Nro. lineas saldo anterior (transac. especificas restantes)", ArrMontosAntesSpecificRestantes.length);
            if (ArrMontosAntesSpecificRestantes.length != 0) {
              ArrMontosAntes = ArrMontosAntes.concat(ArrMontosAntesSpecificRestantes);
            }
          } else {
            deleteAccountsWithoutPuc(ArrMontosAntes);
          }

          if (ArrMontosAntes.length != 0) {
            ArrMontosAntes = OrdenarArreglo(ArrMontosAntes);
            ArrMontosAntes = AgruparCuentas(ArrMontosAntes);
            //log.debug('ArrMontosAntes', ArrMontosAntes);
          }
        }

        if (ArrMontosActual.length != 0) {
          ArrMontosActual = OrdenarArreglo(ArrMontosActual);
          ArrMontosActual = AgruparCuentas(ArrMontosActual);
        }

        if (paramOchoDigitos == 'T') {
          ArrMontosFinal = obtenerArregloFinalOchoDigitos();
        } else {
          ArrMontosFinal = obtenerArregloFinalSeisDigitos();
        }


        //TODO: validate primary book
        if (feamultibook) {
          if (paramMultibook != 1) {
            if (!flagEmpty) {
              ArrMontosFinal = cambioDeCuentas();
            }
          }
        }

        if (!flagEmpty) {
          ArrMontosFinal = OrdenarArreglo(ArrMontosFinal);
          if (paramOchoDigitos == 'T') {
            agregarArregloSeisDigitos();
          } else {
            agregarArregloCuatroDigitos();
          }
        }

        ArrMontosFinal = OrdenarArreglo(ArrMontosFinal);
        //log.debug('ArrMontosFinal',ArrMontosFinal);

        todays = parseDateTo(new Date(), "DATE");
        currentTime = getTimeHardcoded(parseDateTo(new Date(), "DATETIME"));

        if (!feamultibook) {
          deleteAccountsWithoutPuc(ArrMontosFinal);
        }
        orderBalance();
        generateExcel();

        if (flagPDF) {
          generarPDF();
        }
      } catch (error) {
        log.error('Error al procesar Schedule.', error);
        libraryRPT.sendErrorEmail(error, LMRY_script, language);
      }

    }

    function generateExcel() {
      var subArrMontosFinal = [];
      var flagGenerate = true;
      var range = 10000;
      var maxLines = 0;
      var domain = ArrMontosFinal.length;
      var minLines = 0;
      var count = 0;
      while (flagGenerate) {
        count++;
        if (domain == 0) {
          GeneraLibroMayorBalance(ArrMontosFinal, count);
          flagGenerate = false;
        } else if (domain >= range) {
          if (domain == range) {
            flagGenerate = false;
          }
          maxLines += range;
          subArrMontosFinal = ArrMontosFinal.slice(minLines, maxLines);

          GeneraLibroMayorBalance(subArrMontosFinal, count);
          domain -= range;
          minLines = maxLines;
        } else {
          maxLines += domain;
          subArrMontosFinal = ArrMontosFinal.slice(minLines, maxLines);

          GeneraLibroMayorBalance(subArrMontosFinal, count);
          flagGenerate = false;
        }

      }
    }


    function generarPDF() {
      if (featAccountingSpecial || featAccountingSpecial == true) {
        periodstartdate = periodstartdateSpecial;
        periodenddate = periodenddateSpecial;
      }
      if (ArrMontosFinal.length != null && ArrMontosFinal.length != 0 && !flagEmpty) {
        periodstartdate = ordenarFormatoFechas(periodstartdate);
        periodenddate = ordenarFormatoFechas(periodenddate);
        var _TotalSIDebe = 0.0;
        var _TotalSIHaber = 0.0;
        var _TotalMovDebe = 0.0;
        var _TotalMovHaber = 0.0;
        var _TotalSFDebe = 0.0;
        var _TotalSFHaber = 0.0;

        var sal_anterior = 0;
        var mov = 0;
        var nuevo_saldo = 0;
        var contador = 0;

        var arrTransaction = [];
        for (var i = 0; i < ArrMontosFinal.length; i++) {
          if (Math.abs(ArrMontosFinal[i][2]) == 0 && Math.abs(ArrMontosFinal[i][3]) == 0 &&
            Math.abs(ArrMontosFinal[i][4]) == 0 && Math.abs(ArrMontosFinal[i][5]) == 0 &&
            Math.abs(ArrMontosFinal[i][6]) == 0 && Math.abs(ArrMontosFinal[i][7]) == 0) {
            contador++;
          } else {

            //////////////////////////////////
            //numero de cuenta
            if (ArrMontosFinal[i][0] != '' || ArrMontosFinal[i][0] != null) {
              var colum1 = ArrMontosFinal[i][0];
            } else {
              var colum1 = '';
            }

            //Denominacion
            if (ArrMontosFinal[i][1].length > 0) {
              var s = ValidarAcentos(ArrMontosFinal[i][1]);
              var colum2 = s;
            } else {
              var colum2 = '';
            }

            //Document
            var document = '';

            if (paramOchoDigitos == 'T' && ArrMontosFinal[i][19].length > 0) {
              document = ValidarAcentos(ArrMontosFinal[i][19]);
            } else if (paramOchoDigitos != 'T' && ArrMontosFinal[i][17].length > 0) {
              document = ValidarAcentos(ArrMontosFinal[i][17]);
            }

            /////////////////////////////////
            if (ArrMontosFinal[i][0].length == 1 || ArrMontosFinal[i][0].length == 2 || ArrMontosFinal[i][0].length == 4 || ArrMontosFinal[i][0].length == 6 || ArrMontosFinal[i][0].length == 8) {
              var saldo_antes = redondear(Math.abs(ArrMontosFinal[i][2]) - Math.abs(ArrMontosFinal[i][3]));
              var saldo_actual = redondear(Math.abs(ArrMontosFinal[i][2]) + Math.abs(ArrMontosFinal[i][4]) - Math.abs(ArrMontosFinal[i][3]) - Math.abs(ArrMontosFinal[i][5]));

              if (saldo_antes < 0) {
                //Haber Antes
                var colum3 = 0.0;
                //Debe Antes
                var colum4 = Math.abs(saldo_antes);
              } else {
                if (saldo_antes > 0) {
                  //Haber Antes
                  var colum3 = Math.abs(saldo_antes);
                  //Debe Antes
                  var colum4 = 0.0;
                } else {
                  var colum3 = 0.0;
                  var colum4 = 0.0;

                }
              }

              //Haber Movimientos
              var colum5 = Math.abs(ArrMontosFinal[i][4]);
              //Debe Movimientos
              var colum6 = Math.abs(ArrMontosFinal[i][5]);

              if (saldo_actual < 0) {
                //Haber Saldo
                var colum7 = 0.0;

                //Debe Saldo
                var colum8 = Math.abs(saldo_actual);
              } else if (saldo_actual > 0) {
                //Haber Saldo
                var colum7 = Math.abs(saldo_actual);

                //Debe Saldo
                var colum8 = 0.0;

              }

              if (ArrMontosFinal[i][0].length == 1) {
                sal_anterior = Math.abs(ArrMontosFinal[i][2]) - Math.abs(ArrMontosFinal[i][3]);
                mov = Math.abs(ArrMontosFinal[i][4]) - Math.abs(ArrMontosFinal[i][5]);
                nuevo_saldo = Math.abs(ArrMontosFinal[i][6]) - Math.abs(ArrMontosFinal[i][7]);

                if (sal_anterior > 0) {
                  _TotalSIDebe += sal_anterior;
                } else {
                  _TotalSIHaber += sal_anterior;
                }

                _TotalMovDebe += Math.abs(ArrMontosFinal[i][4]);

                _TotalMovHaber += Math.abs(ArrMontosFinal[i][5]);

                /*if(mov>0){
                    _TotalMovDebe += mov;
                }else{
                    _TotalMovHaber += mov;
                }*/

                if (nuevo_saldo > 0) {
                  _TotalSFDebe += nuevo_saldo;
                } else {
                  _TotalSFHaber += nuevo_saldo;
                }
                /*
                    _TotalSIDebe += ArrMontosFinal[i][3];
                    _TotalSIHaber += ArrMontosFinal[i][2];
                    _TotalMovDebe += ArrMontosFinal[i][4];
                    _TotalMovHaber += ArrMontosFinal[i][5];
                    _TotalSFDebe += ArrMontosFinal[i][6];
                    _TotalSFHaber += ArrMontosFinal[i][7];
                    */
              }
            } else {
              //Haber Antes
              var colum3 = Math.abs(ArrMontosFinal[i][2]);
              //Debe Antes
              var colum4 = Math.abs(ArrMontosFinal[i][3]);
              //Haber Movimientos
              var colum5 = Math.abs(ArrMontosFinal[i][4]);
              //Debe Movimientos
              var colum6 = Math.abs(ArrMontosFinal[i][5]);
              //Haber Saldo
              var colum7 = Math.abs(ArrMontosFinal[i][6]);
              //Debe Saldo
              var colum8 = Math.abs(ArrMontosFinal[i][7]);
            }

            arrTransaction.push({
              "colum1": colum1,
              "colum2": colum2,
              "colum3": redondear(colum3),
              "colum4": redondear(colum4),
              "colum5": redondear(colum5),
              "colum6": redondear(colum6),
              "colum7": redondear(colum7),
              "colum8": redondear(colum8),
              "document": document
            });
          }
        }

        var arrTotals = {
          "colum1": redondear(Math.abs(_TotalSIDebe)),
          "colum2": redondear(Math.abs(_TotalSIHaber)),
          "colum3": redondear(Math.abs(_TotalMovDebe)),
          "colum4": redondear(Math.abs(_TotalMovHaber)),
          "colum5": redondear(Math.abs(_TotalSFDebe)),
          "colum6": redondear(Math.abs(_TotalSFHaber)),
        };

        var JsonTraslate = {
          "colum1": GLOBAL_LABELS["Alert5"][language],
          "colum2": GLOBAL_LABELS["Alert6"][language],
          "colum3": GLOBAL_LABELS["Alert7"][language],
          "colum4": GLOBAL_LABELS["Alert8"][language],
          "colum5": GLOBAL_LABELS["Alert9"][language],
          "colum6": GLOBAL_LABELS["Alert10"][language],
          "colum7": GLOBAL_LABELS["Alert11"][language],
          "document": GLOBAL_LABELS["Alert14"][language],
          "totales": GLOBAL_LABELS["Alert13"][language]
        }

        var jsonAxiliar = {
          "company": {
            "title": GLOBAL_LABELS["Alert1"][language],
            "razon": GLOBAL_LABELS["Alert2"][language] + companyname.replace(/&/g, '&amp;'),
            "ruc": 'NIT: ' + companyruc,
            "date": GLOBAL_LABELS["Alert3"][language] + periodstartdate + ' al ' + periodenddate,
          },
          "traslate": JsonTraslate,
          "movements": arrTransaction,
          "total": arrTotals,
          "pdfStandard": {
            "origin": GLOBAL_LABELS["origin"][language] + "Netsuite",
            "todays": GLOBAL_LABELS["date"][language] + todays,
            "currentTime": GLOBAL_LABELS["time"][language] + currentTime,
            "page": GLOBAL_LABELS["page"][language],
            "of": GLOBAL_LABELS["of"][language]
          }
        }

        if ((feamultibook || feamultibook == 'T') && (paramMultibook != '' && paramMultibook != null)) {
          jsonAxiliar["company"].mlb = GLOBAL_LABELS["Alert4"][language] + multibook_name.name;
        } else {
          jsonAxiliar["company"].mlb = '';
        }

        if (paramMultibook != '' && paramMultibook != null) {
          var NameFile = "COLibroMayorBalance_" + companyname + "_" + monthStartD + "_" + yearStartD + "_" + paramMultibook + ".pdf";
        } else {
          var NameFile = "COLibroMayorBalance_" + companyname + "_" + monthStartD + "_" + yearStartD + ".pdf";
        }

        var renderer = render.create();

        renderer.templateContent = getTemplate();

        renderer.addCustomDataSource({
          format: render.DataSource.OBJECT,
          alias: "input",
          data: {
            data: JSON.stringify(jsonAxiliar)
          }
        });

        stringXML = renderer.renderAsPdf();
        saveFilePDF(stringXML, NameFile);
      }

    }

    function saveFilePDF(stringXML, nameReport) {
      var fileAuxliar = stringXML;
      var NameFile = nameReport;
      var folderID = objContext.getParameter({
        name: 'custscript_lmry_file_cabinet_rg_co'
      });

      if (folderID != '' && folderID != null) {

        fileAuxliar.name = NameFile;
        fileAuxliar.folder = folderID;

        // Termina de grabar el archivo
        var idfile = fileAuxliar.save();

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

        //Genera registro personalizado como log
        if (idfile) {
          var usuarioTemp = runtime.getCurrentUser();
          var id = usuarioTemp.id;
          var employeename = search.lookupFields({
            type: search.Type.EMPLOYEE,
            id: id,
            columns: ['firstname', 'lastname']
          });
          var usuario = employeename.firstname + ' ' + employeename.lastname;

          var record = recordModulo.create({
            type: 'customrecord_lmry_co_rpt_generator_log'
          });
          //Nombre de Archivo
          record.setValue({
            fieldId: 'custrecord_lmry_co_rg_name',
            value: NameFile
          });
          //Nombre de Reporte
          record.setValue({
            fieldId: 'custrecord_lmry_co_rg_transaction',
            value: 'CO - Libro Mayor y Balance (Detallado)'
          });
          //Periodo
          record.setValue({
            fieldId: 'custrecord_lmry_co_rg_postingperiod',
            value: periodname
          });
          //Nombre de Subsidiaria
          record.setValue({
            fieldId: 'custrecord_lmry_co_rg_subsidiary',
            value: companyname
          });
          //Url de Archivo
          record.setValue({
            fieldId: 'custrecord_lmry_co_rg_url_file',
            value: urlfile
          });
          //Multibook
          if (feamultibook || feamultibook == 'T') {
            record.setValue({
              fieldId: 'custrecord_lmry_co_rg_multibook',
              value: multibook_name.name
            });
          }
          //Creado Por
          record.setValue({
            fieldId: 'custrecord_lmry_co_rg_employee',
            value: usuario
          });

          var recordId = record.save();
          libraryRPT.sendConfirmUserEmail('Latam CO - Libro Mayor', 3, NameFile, language);
        }
      }
    }

    function getTemplate() {
      var aux = fileModulo.load("./CO_MayorBalanceDetallado_TemplatePDF.xml");
      return aux.getContents();
    }

    function getLibraryRPT() {
      try {

        require(["/SuiteBundles/Bundle 37714/Latam_Library/LMRY_LibraryReport_LBRY_V2.js"], function (library) {
          libraryRPT = library;
        });
        log.debug('[getLibraryRPT] libraryRPT', 'Bundle 37714');

      } catch (err) {

        try {
          require(["/SuiteBundles/Bundle 35754/Latam_Library/LMRY_LibraryReport_LBRY_V2.js"], function (library) {
            libraryRPT = library;
          });

          log.debug('[getLibraryRPT] libraryRPT', 'Bundle 35754');
        } catch (err) {

          log.error('[getLibraryRPT] libraryRPT', 'No se encuentra libreria');
        }
      }

    }

    function getInternalIdPEJ() {
      var internalIds = new Array();
      var savedsearchPEJ = search.create({
        type: "transaction",
        filters:
          [
            ["posting", "is", "T"],
            "AND",
            ["type", "anyof", "PEJrnl"],
            "AND",
            ["mainline", "is", "T"]
          ],
        columns:
          [
            search.createColumn({ name: "internalid", label: "Internal ID" })
          ],
        settings:
          [
            {
              name: 'includeperiodendtransactions',
              value: 'TRUE'
            }
          ]
      });
      if ((feamultibook || feamultibook == 'T') && (paramMultibook != '' || paramMultibook != null)) {
        savedsearchPEJ.filters.push(search.createFilter({
          name: 'accountingbook',
          join: 'accountingtransaction',
          operator: search.Operator.ANYOF,
          values: [paramMultibook]
        }));
      }
      if (featuresubs) {
        savedsearchPEJ.filters.push(search.createFilter({
          name: 'subsidiary',
          operator: search.Operator.IS,
          values: [paramsubsidi]
        }));
      }

      //para saldo anterior
      savedsearchPEJ.filters.push(search.createFilter({
        name: 'startdate',
        join: 'accountingperiod',
        operator: search.Operator.BEFORE,
        values: [periodstartdate]
      }));

      if (paramOchoDigitos == 'T') {
        savedsearchPEJ.filters.push(search.createFilter({
          name: "formulatext",
          formula: "LENGTH({account.custrecord_lmry_co_puc_id})",
          operator: "is",
          values: 8
        }));
      }


      var pageData = savedsearchPEJ.runPaged({
        pageSize: 1000
      });

      pageData.pageRanges.forEach(function (pageRange) {
        page = pageData.fetch({
          index: pageRange.index
        });
        page.data.forEach(function (result) {

          var columns = result.columns;
          // 0.Internal id
          var internalId = result.getValue(columns[0]);

          internalIds.push(internalId);

        });
      });
      return internalIds;
    }

    function getInternalIdPEJRestantesPUCs4567() {
      var internalIds = new Array();
      var savedsearchPEJ = search.create({
        type: "transaction",
        filters:
          [
            ["posting", "is", "T"],
            "AND",
            ["type", "anyof", "PEJrnl"],
            "AND",
            ["mainline", "is", "T"]
          ],
        columns:
          [
            search.createColumn({ name: "internalid", label: "Internal ID" })
          ],
        settings:
          [
            {
              name: 'includeperiodendtransactions',
              value: 'TRUE'
            }
          ]
      });
      if ((feamultibook || feamultibook == 'T') && (paramMultibook != '' || paramMultibook != null)) {
        savedsearchPEJ.filters.push(search.createFilter({
          name: 'accountingbook',
          join: 'accountingtransaction',
          operator: search.Operator.ANYOF,
          values: [paramMultibook]
        }));
      }
      if (featuresubs) {
        savedsearchPEJ.filters.push(search.createFilter({
          name: 'subsidiary',
          operator: search.Operator.IS,
          values: [paramsubsidi]
        }));
      }

      // Saldo Anterior Restantes para PUCs 4,5,6 y 7 (Cuentas de Resultados)
      var period_year_init = format.parse({
        value: periodstartdate,
        type: format.Type.DATE
      }).getFullYear();
      var januaryFisrt = new Date(period_year_init, 0, 1);
      var format_january_first = format.format({
        value: januaryFisrt,
        type: format.Type.DATE
      });

      //! Obtiene los periodos anteriores, incluidos los periodos de ajuste
      var arrPeriods = getRangeDate(format_january_first, periodstartdate);

      //Si no hay periodos anteiores se regresa un [] como saldos anteriores
      if (arrPeriods.length == 0) {
        return internalIds;
      }

      var formula = ObtenerFormulaFiltroPeriodo(arrPeriods, 1);
      var periodFilter = search.createFilter({
        name: 'formulatext',
        operator: search.Operator.IS,
        values: '1',
        formula: formula
      });
      savedsearchPEJ.filters.push(periodFilter);

      if (paramOchoDigitos == 'T') {
        savedsearchPEJ.filters.push(search.createFilter({
          name: "formulatext",
          formula: "LENGTH({account.custrecord_lmry_co_puc_id})",
          operator: "is",
          values: 8
        }));
      }


      var pageData = savedsearchPEJ.runPaged({
        pageSize: 1000
      });

      pageData.pageRanges.forEach(function (pageRange) {
        page = pageData.fetch({
          index: pageRange.index
        });
        page.data.forEach(function (result) {

          var columns = result.columns;
          // 0.Internal id
          var internalId = result.getValue(columns[0]);

          internalIds.push(internalId);

        });
      });
      return internalIds;
    }

    function getPEJ(internalIds, type) {
      var intDMinReg = paramBucle * 1000;
      var intDMaxReg = intDMinReg + 1000;
      // Exedio las unidades
      var DbolStop = false;
      var ArrMontosAux = new Array();
      var searchPEJ = search.create({
        type: "accountingtransaction",
        filters:
          [
            ["transaction.posting", "is", "T"],
            "AND",
            ["transaction.internalid", "anyof", internalIds]
          ],
        columns:
          [
            search.createColumn({
              name: "formulatext",
              summary: "GROUP",
              formula: "{account.custrecord_lmry_co_puc_d6_id}",
              label: "6 digitos"
            }),
            search.createColumn({
              name: "formulatext",
              summary: "GROUP",
              formula: "{account.custrecord_lmry_co_puc_d6_description}",
              label: "6 Digitos des"
            }),
            search.createColumn({
              name: "formulacurrency",
              summary: "SUM",
              formula: "NVL({debitamount},0)",
              label: "Formula (Currency)"
            }),
            search.createColumn({
              name: "formulacurrency",
              summary: "SUM",
              formula: "NVL({creditamount},0)",
              label: "Formula (Currency)"
            }),
            search.createColumn({
              name: "formulatext",
              summary: "GROUP",
              formula: "{account.custrecord_lmry_co_puc_d4_id}",
              label: "4 digitos"
            }),
            search.createColumn({
              name: "formulatext",
              summary: "GROUP",
              formula: "{account.custrecord_lmry_co_puc_d4_description}",
              label: "4 digitos des"
            }),
            search.createColumn({
              name: "formulatext",
              summary: "GROUP",
              formula: "{account.custrecord_lmry_co_puc_d2_id}",
              label: "2 digitos"
            }),
            search.createColumn({
              name: "formulatext",
              summary: "GROUP",
              formula: "{account.custrecord_lmry_co_puc_d2_description}",
              label: "2 digitos des"
            }),
            search.createColumn({
              name: "formulatext",
              summary: "GROUP",
              formula: "{account.custrecord_lmry_co_puc_d1_id}",
              label: "1 digito"
            }),
            search.createColumn({
              name: "formulatext",
              summary: "GROUP",
              formula: "{account.custrecord_lmry_co_puc_d1_description}",
              label: "1 digito des"
            }),
            search.createColumn({
              name: "formulacurrency",
              summary: "SUM",
              formula: "NVL({debitamount},0) - NVL({creditamount},0)",
              label: "Formula (Currency)"
            }),
            search.createColumn({
              name: "accounttype",
              summary: "GROUP",
              label: "Account Type"
            }),
            search.createColumn({
              name: "number",
              join: "account",
              summary: "GROUP",
              label: "Number"
            }),
          ],
        settings:
          [
            {
              name: 'includeperiodendtransactions',
              value: 'TRUE'
            }
          ]
      });

      //13. traind
      searchPEJ.columns.push(search.createColumn({
        name: 'formulatext',
        formula: 'NVL({transaction.tranid},{transaction.transactionnumber})',
        summary: 'GROUP',
        label: 'Tran Id or transaction number'
      }));

      //14. type
      searchPEJ.columns.push(search.createColumn({
        name: "type",
        join: "transaction",
        summary: 'GROUP',
        label: "DOCUMENTO"
      }));


      if (paramOchoDigitos == 'T') {
        searchPEJ.columns.push(search.createColumn({
          name: "formulatext",
          summary: "GROUP",
          formula: "{account.custrecord_lmry_co_puc_id}",
          sort: search.Sort.ASC,
          label: "PUC 8D ID"
        }));
      }

      var searchresult = searchPEJ.run();

      while (!DbolStop) {
        var objResult = searchresult.getRange(intDMinReg, intDMaxReg);

        if (objResult != null) {
          var intLength = objResult.length;


          for (var i = 0; i < intLength; i++) {
            var columns = objResult[i].columns;
            var arrAuxiliar = new Array();

            if (paramOchoDigitos == 'T') {
              //0. cuenta 8 digitos
              if (objResult[i].getValue(columns[15]) != null && objResult[i].getValue(columns[15]) != '- None -' && objResult[i].getValue(columns[15]) != 'NaN' && objResult[i].getValue(columns[15]) != 'undefined')
                arrAuxiliar[0] = objResult[i].getValue(columns[15]);
              else
                arrAuxiliar[0] = '';
              //1. denominacion 8 digitos
              var descripcion8digitos = puc8DigDescript[arrAuxiliar[0]];
              if (descripcion8digitos != null && descripcion8digitos != '- None -' && descripcion8digitos != 'NaN' && descripcion8digitos != 'undefined')
                arrAuxiliar[1] = ValidarAcentos(descripcion8digitos);
              else
                arrAuxiliar[1] = '';

              //2. cuenta 6 digitos
              if (objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '- None -' && objResult[i].getValue(columns[0]) != 'NaN' && objResult[i].getValue(columns[0]) != 'undefined')
                arrAuxiliar[2] = objResult[i].getValue(columns[0]);
              else
                arrAuxiliar[2] = '';

              //3. denominacion 6 digitos
              if (objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '- None -' && objResult[i].getValue(columns[1]) != 'NaN' && objResult[i].getValue(columns[1]) != 'undefined')
                arrAuxiliar[3] = ValidarAcentos(objResult[i].getValue(columns[1]));
              else
                arrAuxiliar[3] = '';

              //4.  sum debitos
              if (objResult[i].getValue(columns[2]) != null && objResult[i].getValue(columns[2]) != '- None -' && objResult[i].getValue(columns[2]) != 'NaN' && objResult[i].getValue(columns[2]) != 'undefined')
                arrAuxiliar[4] = objResult[i].getValue(columns[2]);
              else
                arrAuxiliar[4] = 0.0;


              //5.  sum credito
              if (objResult[i].getValue(columns[3]) != null && objResult[i].getValue(columns[3]) != '- None -' && objResult[i].getValue(columns[3]) != 'NaN' && objResult[i].getValue(columns[3]) != 'undefined')
                arrAuxiliar[5] = objResult[i].getValue(columns[3]);
              else
                arrAuxiliar[5] = 0.0;


              //6.  cuenta 4 digitos
              if (objResult[i].getValue(columns[4]) != null && objResult[i].getValue(columns[4]) != '- None -' && objResult[i].getValue(columns[4]) != 'NaN' && objResult[i].getValue(columns[4]) != 'undefined')
                arrAuxiliar[6] = objResult[i].getValue(columns[4]);
              else
                arrAuxiliar[6] = '';

              //7.  denominacion 4 digitos
              if (objResult[i].getValue(columns[5]) != null && objResult[i].getValue(columns[5]) != '- None -' && objResult[i].getValue(columns[5]) != 'NaN' && objResult[i].getValue(columns[5]) != 'undefined')
                arrAuxiliar[7] = ValidarAcentos(objResult[i].getValue(columns[5]));
              else
                arrAuxiliar[7] = '';

              //8.  cuenta 2 digitos
              if (objResult[i].getValue(columns[6]) != null && objResult[i].getValue(columns[6]) != '- None -' && objResult[i].getValue(columns[6]) != 'NaN' && objResult[i].getValue(columns[6]) != 'undefined')
                arrAuxiliar[8] = objResult[i].getValue(columns[6]);
              else
                arrAuxiliar[8] = '';

              //9.  denominacion 2 digitos
              if (objResult[i].getValue(columns[7]) != null && objResult[i].getValue(columns[7]) != '- None -' && objResult[i].getValue(columns[7]) != 'NaN' && objResult[i].getValue(columns[7]) != 'undefined')
                arrAuxiliar[9] = ValidarAcentos(objResult[i].getValue(columns[7]));
              else
                arrAuxiliar[9] = '';

              //10.  cuenta 1 digito
              if (objResult[i].getValue(columns[8]) != null && objResult[i].getValue(columns[8]) != '- None -' && objResult[i].getValue(columns[8]) != 'NaN' && objResult[i].getValue(columns[8]) != 'undefined')
                arrAuxiliar[10] = objResult[i].getValue(columns[8]);
              else
                arrAuxiliar[10] = '';

              //11.  denominacion 1 digito
              if (objResult[i].getValue(columns[9]) != null && objResult[i].getValue(columns[9]) != '- None -' && objResult[i].getValue(columns[9]) != 'NaN' && objResult[i].getValue(columns[9]) != 'undefined')
                arrAuxiliar[11] = ValidarAcentos(objResult[i].getValue(columns[9]));
              else
                arrAuxiliar[11] = '';

              //12. saldo
              arrAuxiliar[12] = arrAuxiliar[2] - arrAuxiliar[3];

              //13. tipo cuenta
              if (objResult[i].getValue(columns[11]) != null && objResult[i].getValue(columns[11]) != '- None -' && objResult[i].getValue(columns[11]) != 'NaN' && objResult[i].getValue(columns[11]) != 'undefined')
                arrAuxiliar[13] = ValidarAcentos(objResult[i].getText(columns[11]));
              else
                arrAuxiliar[13] = '';

              //14. number
              if (objResult[i].getValue(columns[12]) != null && objResult[i].getValue(columns[12]) != '- None -' && objResult[i].getValue(columns[12]) != 'NaN' && objResult[i].getValue(columns[12]) != 'undefined')
                arrAuxiliar[14] = objResult[i].getValue(columns[12]);
              else
                arrAuxiliar[14] = '';


              //15 Document
              var val_trainid = objResult[i].getValue(columns[13]) != null && objResult[i].getValue(columns[13]) != '- None -' && objResult[i].getValue(columns[13]) != 'NaN' && objResult[i].getValue(columns[13]) != 'undefined';

              var val_type = objResult[i].getValue(columns[14]) != null && objResult[i].getValue(columns[14]) != '- None -' && objResult[i].getValue(columns[14]) != 'NaN' && objResult[i].getValue(columns[14]) != 'undefined';

              if (val_type)
                arrAuxiliar[15] = objResult[i].getText(columns[14]);
              else
                arrAuxiliar[15] = '';

              if (val_trainid)
                arrAuxiliar[15] += " - " + objResult[i].getValue(columns[13]);

            } else {
              //0. cuenta 6 digitos
              if (objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '- None -' && objResult[i].getValue(columns[0]) != 'NaN' && objResult[i].getValue(columns[0]) != 'undefined')
                arrAuxiliar[0] = objResult[i].getValue(columns[0]);
              else
                arrAuxiliar[0] = '';

              //1. denominacion 6 digitos
              if (objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '- None -' && objResult[i].getValue(columns[1]) != 'NaN' && objResult[i].getValue(columns[1]) != 'undefined')
                arrAuxiliar[1] = ValidarAcentos(objResult[i].getValue(columns[1]));
              else
                arrAuxiliar[1] = '';

              //2.  sum debitos

              if (objResult[i].getValue(columns[2]) != null && objResult[i].getValue(columns[2]) != '- None -' && objResult[i].getValue(columns[2]) != 'NaN' && objResult[i].getValue(columns[2]) != 'undefined')
                arrAuxiliar[2] = objResult[i].getValue(columns[2]);
              else
                arrAuxiliar[2] = 0.0;


              //3.  sum credito

              if (objResult[i].getValue(columns[3]) != null && objResult[i].getValue(columns[3]) != '- None -' && objResult[i].getValue(columns[3]) != 'NaN' && objResult[i].getValue(columns[3]) != 'undefined')
                arrAuxiliar[3] = objResult[i].getValue(columns[3]);
              else
                arrAuxiliar[3] = 0.0;


              //4.  cuenta 4 digitos
              if (objResult[i].getValue(columns[4]) != null && objResult[i].getValue(columns[4]) != '- None -' && objResult[i].getValue(columns[4]) != 'NaN' && objResult[i].getValue(columns[4]) != 'undefined')
                arrAuxiliar[4] = objResult[i].getValue(columns[4]);
              else
                arrAuxiliar[4] = '';

              //5.  denominacion 4 digitos
              if (objResult[i].getValue(columns[5]) != null && objResult[i].getValue(columns[5]) != '- None -' && objResult[i].getValue(columns[5]) != 'NaN' && objResult[i].getValue(columns[5]) != 'undefined')
                arrAuxiliar[5] = ValidarAcentos(objResult[i].getValue(columns[5]));
              else
                arrAuxiliar[5] = '';

              //6.  cuenta 2 digitos
              if (objResult[i].getValue(columns[6]) != null && objResult[i].getValue(columns[6]) != '- None -' && objResult[i].getValue(columns[6]) != 'NaN' && objResult[i].getValue(columns[6]) != 'undefined')
                arrAuxiliar[6] = objResult[i].getValue(columns[6]);
              else
                arrAuxiliar[6] = '';

              //7.  denominacion 2 digitos
              if (objResult[i].getValue(columns[7]) != null && objResult[i].getValue(columns[7]) != '- None -' && objResult[i].getValue(columns[7]) != 'NaN' && objResult[i].getValue(columns[7]) != 'undefined')
                arrAuxiliar[7] = ValidarAcentos(objResult[i].getValue(columns[7]));
              else
                arrAuxiliar[7] = '';

              //8.  cuenta 1 digito
              if (objResult[i].getValue(columns[8]) != null && objResult[i].getValue(columns[8]) != '- None -' && objResult[i].getValue(columns[8]) != 'NaN' && objResult[i].getValue(columns[8]) != 'undefined')
                arrAuxiliar[8] = objResult[i].getValue(columns[8]);
              else
                arrAuxiliar[8] = '';

              //9.  denominacion 1 digito
              if (objResult[i].getValue(columns[9]) != null && objResult[i].getValue(columns[9]) != '- None -' && objResult[i].getValue(columns[9]) != 'NaN' && objResult[i].getValue(columns[9]) != 'undefined')
                arrAuxiliar[9] = ValidarAcentos(objResult[i].getValue(columns[9]));
              else
                arrAuxiliar[9] = '';

              //10. saldo

              arrAuxiliar[10] = arrAuxiliar[2] - arrAuxiliar[3];


              //11. tipo cuenta
              if (objResult[i].getValue(columns[11]) != null && objResult[i].getValue(columns[11]) != '- None -' && objResult[i].getValue(columns[11]) != 'NaN' && objResult[i].getValue(columns[11]) != 'undefined')
                arrAuxiliar[11] = ValidarAcentos(objResult[i].getText(columns[11]));
              else
                arrAuxiliar[11] = '';

              //12. number

              if (objResult[i].getValue(columns[12]) != null && objResult[i].getValue(columns[12]) != '- None -' && objResult[i].getValue(columns[12]) != 'NaN' && objResult[i].getValue(columns[12]) != 'undefined')
                arrAuxiliar[12] = objResult[i].getValue(columns[12]);
              else
                arrAuxiliar[12] = '';


              //13 Document

              var val_trainid = objResult[i].getValue(columns[13]) != null && objResult[i].getValue(columns[13]) != '- None -' && objResult[i].getValue(columns[13]) != 'NaN' && objResult[i].getValue(columns[13]) != 'undefined';

              var val_type = objResult[i].getValue(columns[14]) != null && objResult[i].getValue(columns[14]) != '- None -' && objResult[i].getValue(columns[14]) != 'NaN' && objResult[i].getValue(columns[14]) != 'undefined';

              if (val_type)
                arrAuxiliar[13] = objResult[i].getText(columns[14]);
              else
                arrAuxiliar[13] = '';

              if (val_trainid)
                arrAuxiliar[13] += " - " + objResult[i].getValue(columns[13]);

            }

            if (feamultibook) {
              //Cambios de cuenta multibook para saldos anteriores
              if (paramOchoDigitos == 'T') {
                var x = jsonIndexCuentas.get(arrAuxiliar[14]);
                if (x != undefined) {
                  arrAuxiliar[0] = ArrCuentas[x][10];
                  arrAuxiliar[1] = ArrCuentas[x][11];
                  arrAuxiliar[2] = ArrCuentas[x][8];
                  arrAuxiliar[3] = ArrCuentas[x][9];
                  arrAuxiliar[6] = ArrCuentas[x][6];
                  arrAuxiliar[7] = ArrCuentas[x][7];
                  arrAuxiliar[8] = ArrCuentas[x][4];
                  arrAuxiliar[9] = ArrCuentas[x][5];
                  arrAuxiliar[10] = ArrCuentas[x][2];
                  arrAuxiliar[11] = ArrCuentas[x][3];
                }
              } else {
                var x = jsonIndexCuentas.get(arrAuxiliar[12]);
                if (x != undefined) {
                  arrAuxiliar[0] = ArrCuentas[x][8];
                  arrAuxiliar[1] = ArrCuentas[x][9];
                  arrAuxiliar[4] = ArrCuentas[x][6];
                  arrAuxiliar[5] = ArrCuentas[x][7];
                  arrAuxiliar[6] = ArrCuentas[x][4];
                  arrAuxiliar[7] = ArrCuentas[x][5];
                  arrAuxiliar[8] = ArrCuentas[x][2];
                  arrAuxiliar[9] = ArrCuentas[x][3];
                }
              }
            }

            if (type == 2) {
              var tempPuc = arrAuxiliar[0].substring(0, 1);
              if (tempPuc == 4 || tempPuc == 5 || tempPuc == 6 || tempPuc == 7) {
                ArrMontosAux.push(arrAuxiliar);
              }
            } else {
              var tempPuc = arrAuxiliar[0].substring(0, 1);
              if (tempPuc != 4 && tempPuc != 5 && tempPuc != 6 && tempPuc != 7) {
                ArrMontosAux.push(arrAuxiliar);
              }
            }
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
      return ArrMontosAux;
    }

    function getSaldoAnteriorPEJ(type) {
      if (type == 1) {
        var internalIds = getInternalIdPEJ();
      } else {
        var internalIds = getInternalIdPEJRestantesPUCs4567();
      }
      if (internalIds.length != 0) {
        var ArrMontosAux = getPEJ(internalIds, type);
        return ArrMontosAux;
      }
      return [];
    }

    function CambiarCuentasMultibook(arrTemporal) {
      for (var i = 0; i < arrTemporal.length; i++) {
        for (var j = 0; j < ArrCuentas.length; j++) {

          if (paramOchoDigitos == 'T') {
            if (arrTemporal[i][14] == ArrCuentas[j][0]) {
              arrTemporal[i][0] = ArrCuentas[j][10];
              arrTemporal[i][1] = ArrCuentas[j][11];
              arrTemporal[i][2] = ArrCuentas[j][8];
              arrTemporal[i][3] = ArrCuentas[j][9];
              arrTemporal[i][6] = ArrCuentas[j][6];
              arrTemporal[i][7] = ArrCuentas[j][7];
              arrTemporal[i][8] = ArrCuentas[j][4];
              arrTemporal[i][9] = ArrCuentas[j][5];
              arrTemporal[i][10] = ArrCuentas[j][2];
              arrTemporal[i][11] = ArrCuentas[j][3];
            }
          } else {
            if (arrTemporal[i][12] == ArrCuentas[j][0]) {
              arrTemporal[i][0] = ArrCuentas[j][8];
              arrTemporal[i][1] = ArrCuentas[j][9];
              arrTemporal[i][4] = ArrCuentas[j][6];
              arrTemporal[i][5] = ArrCuentas[j][7];
              arrTemporal[i][6] = ArrCuentas[j][4];
              arrTemporal[i][7] = ArrCuentas[j][5];
              arrTemporal[i][8] = ArrCuentas[j][2];
              arrTemporal[i][9] = ArrCuentas[j][3];
            }
          }
        }
      }

      //eliminar cuentas sin puc
      for (var i = 0; i < arrTemporal.length; i++) {
        if (arrTemporal[i][0] == null || arrTemporal[i][0] == '' || arrTemporal[i][0] == '- None -' || arrTemporal[i][0] == '- none -' || arrTemporal[i][0] == 'undefined') {
          arrTemporal.splice(i, 1);
          i--;
        }
      }
      //  log.debug('Arreglo temporal en la funcion CambiarCuentasMultibook', arrTemporal);

    }

    function deleteAccountsWithoutPuc(arrTemporal) {
      for (var i = 0; i < arrTemporal.length; i++) {
        if (arrTemporal[i][0] == null || arrTemporal[i][0] == '' || arrTemporal[i][0] == '- None -' || arrTemporal[i][0] == '- none -' || arrTemporal[i][0] == 'undefined') {
          arrTemporal.splice(i, 1);
          i--;
        }
      }
    }
    function ObtenerCuentas() {
      // Control de Memoria
      var intDMaxReg = 1000;
      var intDMinReg = 0;
      var filters = new Array();


      // Exedio las unidades
      var DbolStop = false;
      var _cont = 0;

      var filters = new Array();

      filters[0] = search.createFilter({
        name: 'isinactive',
        operator: search.Operator.IS,
        values: 'F'
      });

      //propio
      if (paramOchoDigitos == 'T') {
        filters[1] = search.createFilter({
          name: 'formulatext',
          formula: "{custrecord_lmry_co_puc_id}",
          operator: 'isnotempty',
          values: ''
        });

        filters[3] = search.createFilter({
          name: 'formulatext',
          formula: "LENGTH({custrecord_lmry_co_puc_id})",
          operator: 'is',
          values: 8
        });
      } else {
        filters[1] = search.createFilter({
          name: 'formulatext',
          formula: "{custrecord_lmry_co_puc_d6_id}",
          operator: 'isnotempty',
          values: ''
        });
      }

      filters[2] = search.createFilter({
        name: 'subsidiary',
        operator: search.Operator.IS,
        values: [paramsubsidi]
      });

      //propio

      var columns = new Array();

      columns[0] = search.createColumn({
        name: 'internalid'
      });
      columns[1] = search.createColumn({
        name: 'number'
      });
      columns[2] = search.createColumn({
        name: 'custrecord_lmry_co_puc_d1_id'
      });
      columns[3] = search.createColumn({
        name: 'custrecord_lmry_co_puc_d1_description'
      });
      columns[4] = search.createColumn({
        name: 'custrecord_lmry_co_puc_d2_id'
      });
      columns[5] = search.createColumn({
        name: 'custrecord_lmry_co_puc_d2_description'
      });
      columns[6] = search.createColumn({
        name: 'custrecord_lmry_co_puc_d4_id'
      });
      columns[7] = search.createColumn({
        name: 'custrecord_lmry_co_puc_d4_description'
      });
      columns[8] = search.createColumn({
        name: 'custrecord_lmry_co_puc_d6_id'
      });
      columns[9] = search.createColumn({
        name: 'custrecord_lmry_co_puc_d6_description'
      });

      if (paramOchoDigitos == 'T') {
        columns[10] = search.createColumn({
          name: 'custrecord_lmry_co_puc_id'
        });
        columns[11] = search.createColumn({
          name: "formulatext",
          formula: "{custrecord_lmry_co_puc_id.custrecord_lmry_co_puc}",
          label: "PUC 8D DESCRIPTION"
        });
      }

      var savedsearch = search.create({
        type: 'account',
        filters: filters,
        columns: columns
      })

      var searchresult = savedsearch.run();

      while (!DbolStop) {
        var objResult = searchresult.getRange(intDMinReg, intDMaxReg);
        if (objResult != null) {
          var intLength = objResult.length;

          for (var i = 0; i < intLength; i++) {
            columns = objResult[i].columns;

            var arrAuxiliar = new Array();

            //0. Internal Id
            if (objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '' && objResult[i].getValue(columns[0]) != '- None -' && objResult[i].getValue(columns[0]) != 'NaN' && objResult[i].getValue(columns[0]) != 'undefined')
              arrAuxiliar[0] = objResult[i].getValue(columns[0]);
            else
              arrAuxiliar[0] = '';
            //1. number
            if (objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '' && objResult[i].getValue(columns[1]) != '- None -' && objResult[i].getValue(columns[1]) != 'NaN' && objResult[i].getValue(columns[1]) != 'undefined')
              arrAuxiliar[1] = objResult[i].getValue(columns[1]);
            else
              arrAuxiliar[1] = '';
            //2. puc 1 id
            if (objResult[i].getText(columns[2]) != null && objResult[i].getText(columns[2]) != '' && objResult[i].getText(columns[2]) != '- None -' && objResult[i].getText(columns[2]) != 'NaN' && objResult[i].getText(columns[2]) != 'undefined')
              arrAuxiliar[2] = objResult[i].getText(columns[2]);
            else
              arrAuxiliar[2] = '';
            //3. puc 1 des
            if (objResult[i].getValue(columns[3]) != null && objResult[i].getValue(columns[3]) != '' && objResult[i].getValue(columns[3]) != '- None -' && objResult[i].getValue(columns[3]) != 'NaN' && objResult[i].getValue(columns[3]) != 'undefined')
              arrAuxiliar[3] = objResult[i].getValue(columns[3]);
            else
              arrAuxiliar[3] = '';
            //4. puc 2 id
            if (objResult[i].getText(columns[4]) != null && objResult[i].getText(columns[4]) != '' && objResult[i].getText(columns[4]) != '- None -' && objResult[i].getText(columns[4]) != 'NaN' && objResult[i].getText(columns[4]) != 'undefined')
              arrAuxiliar[4] = objResult[i].getText(columns[4]);
            else
              arrAuxiliar[4] = '';
            //5. puc 2 des
            if (objResult[i].getValue(columns[5]) != null && objResult[i].getValue(columns[5]) != '' && objResult[i].getValue(columns[5]) != '- None -' && objResult[i].getValue(columns[5]) != 'NaN' && objResult[i].getValue(columns[5]) != 'undefined')
              arrAuxiliar[5] = objResult[i].getValue(columns[5]);
            else
              arrAuxiliar[5] = '';
            //6. puc 4 id
            if (objResult[i].getText(columns[6]) != null && objResult[i].getText(columns[6]) != '' && objResult[i].getText(columns[6]) != '- None -' && objResult[i].getText(columns[6]) != 'NaN' && objResult[i].getText(columns[6]) != 'undefined')
              arrAuxiliar[6] = objResult[i].getText(columns[6]);
            else
              arrAuxiliar[6] = '';
            //7. puc 4 id
            if (objResult[i].getValue(columns[7]) != null && objResult[i].getValue(columns[7]) != '' && objResult[i].getValue(columns[7]) != '- None -' && objResult[i].getValue(columns[7]) != 'NaN' && objResult[i].getValue(columns[7]) != 'undefined')
              arrAuxiliar[7] = objResult[i].getValue(columns[7]);
            else
              arrAuxiliar[7] = '';
            //8. puc 6 id
            if (objResult[i].getText(columns[8]) != null && objResult[i].getText(columns[8]) != '' && objResult[i].getText(columns[8]) != '- None -' && objResult[i].getText(columns[8]) != 'NaN' && objResult[i].getText(columns[8]) != 'undefined')
              arrAuxiliar[8] = objResult[i].getText(columns[8]);
            else
              arrAuxiliar[8] = '';
            //9. puc 6 id
            if (objResult[i].getValue(columns[9]) != null && objResult[i].getValue(columns[9]) != '' && objResult[i].getValue(columns[9]) != '- None -' && objResult[i].getValue(columns[9]) != 'NaN' && objResult[i].getValue(columns[9]) != 'undefined')
              arrAuxiliar[9] = objResult[i].getValue(columns[9]);
            else
              arrAuxiliar[9] = '';

            if (paramOchoDigitos == 'T') {
              //8. puc 8 id
              if (objResult[i].getText(columns[10]) != null && objResult[i].getText(columns[10]) != '' && objResult[i].getText(columns[10]) != '- None -' && objResult[i].getText(columns[10]) != 'NaN' && objResult[i].getText(columns[10]) != 'undefined')
                arrAuxiliar[10] = objResult[i].getText(columns[10]);
              else
                arrAuxiliar[10] = '';
              //9. puc 8 descripcion
              if (objResult[i].getValue(columns[11]) != null && objResult[i].getValue(columns[11]) != '' && objResult[i].getValue(columns[11]) != '- None -' && objResult[i].getValue(columns[11]) != 'NaN' && objResult[i].getValue(columns[11]) != 'undefined')
                arrAuxiliar[11] = objResult[i].getValue(columns[11]);
              else
                arrAuxiliar[11] = '';
              // ESTOY VALIDANDO CON EL PUC 8D PORQUE EN QA MULTI ALGUNOS PUCS 8D NO TIENEN DESCRIPCION
              if (arrAuxiliar[10] != null && arrAuxiliar[10] != '') {
                ArrCuentas[_cont] = arrAuxiliar;
                jsonIndexCuentas.set(arrAuxiliar[0], _cont);
                _cont++;
              }
            } else {
              if (arrAuxiliar[9] != null && arrAuxiliar[9] != '') {
                ArrCuentas[_cont] = arrAuxiliar;
                jsonIndexCuentas.set(arrAuxiliar[0], _cont);
                _cont++;
              }
            }
            // log.debug('arrAuxiliar',arrAuxiliar);
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
    }

    function ValidarPrimaryBook() {
      var accbook_check = search.lookupFields({
        type: search.Type.ACCOUNTING_BOOK,
        id: paramMultibook,
        columns: 'isprimary'
      })

      return accbook_check.isprimary;
    }

    function ObtieneSpecificTransaction(type, otherType) {
      // Control de Memoria
      var intDMinReg = 0;
      var intDMaxReg = 1000;
      // Exedio las unidades
      var DbolStop = false;
      var ArrMontosAux = new Array();

      var savedsearch = search.load({
        /*Latamready - CO Ledger and balance Multibook*/
        type: 'accountingtransaction',
        id: 'customsearch_lmry_co_mayor_bal_multi',
      })

      savedsearch.filters.push(search.createFilter({
        name: 'accountingbook',
        operator: search.Operator.ANYOF,
        values: [paramMultibook]
      }));

      // Valida si es OneWorld
      if (featuresubs) {
        savedsearch.filters.push(search.createFilter({
          name: 'subsidiary',
          operator: search.Operator.IS,
          values: [paramsubsidi]
        }));
      }

      //Saldo Anterior
      if (!type && otherType == 2) {

        var fecha = ObtenerFormulaFechas();
        var filter_period = search.createFilter({
          name: 'formulatext',
          operator: search.Operator.IS,
          values: '1',
          formula: fecha
        });
        savedsearch.filters.push(filter_period);

      } else if (type && otherType == 1) {
        //Movimiento
        if (paramAdjustment == 'T') {
          if (featurePeriodEnd) {
            var confiPeriodEnd = search.createSetting({
              name: 'includeperiodendtransactions',
              value: 'TRUE'
            })
            savedsearch.settings.push(confiPeriodEnd);
          }
        }
        savedsearch.filters.push(search.createFilter({
          name: 'postingperiod',
          join: 'transaction',
          operator: search.Operator.IS,
          values: [paramperiodo]
        }));

      } else if (!type && otherType == 3) {
        // Saldo Anterior Restantes para PUCs 4,5,6 y 7 (Cuentas de Resultados)
        var period_year_init = format.parse({
          value: periodstartdate,
          type: format.Type.DATE
        }).getFullYear();
        var januaryFisrt = new Date(period_year_init, 0, 1);
        var format_january_first = format.format({
          value: januaryFisrt,
          type: format.Type.DATE
        });

        //! Obtiene los periodos anteriores, incluidos los periodos de ajuste
        var arrPeriods = getRangeDate(format_january_first, periodstartdate);

        //Si no hay periodos anteiores se regresa un [] como saldos anteriores
        if (arrPeriods.length == 0) {
          return ArrMontosAux;
        }

        var formula = ObtenerFormulaFiltroPeriodo(arrPeriods, 2);
        var periodFilter = search.createFilter({
          name: 'formulatext',
          operator: search.Operator.IS,
          values: '1',
          formula: formula
        });
        savedsearch.filters.push(periodFilter);
      }

      //13. traind
      savedsearch.columns.push(search.createColumn({
        name: 'formulatext',
        formula: 'NVL({transaction.tranid},{transaction.transactionnumber})',
        summary: 'GROUP',
        label: 'Tran Id or transaction number'
      }));

      //14. type
      savedsearch.columns.push(search.createColumn({
        name: "type",
        join: "transaction",
        summary: 'GROUP',
        label: "DOCUMENTO"
      }));

      if (paramOchoDigitos == 'T') {
        savedsearch.filters.push(search.createFilter({
          name: "formulatext",
          formula: "LENGTH({account.custrecord_lmry_co_puc_id})",
          operator: "is",
          values: 8
        }));

        // 15. PUC 8DIGITOS
        savedsearch.columns.push(search.createColumn({
          name: "formulatext",
          summary: "GROUP",
          formula: "{account.custrecord_lmry_co_puc_id}",
          sort: search.Sort.ASC,
          label: "PUC 8D ID"
        }));
      }

      var searchresult = savedsearch.run();

      while (!DbolStop) {
        var objResult = searchresult.getRange(intDMinReg, intDMaxReg);

        if (objResult != null) {
          var intLength = objResult.length;

          for (var i = 0; i < intLength; i++) {
            columns = objResult[i].columns;
            var arrAuxiliar = new Array();

            if (paramOchoDigitos == 'T') {

              //0. cuenta 8 digitos
              if (objResult[i].getValue(columns[15]) != null && objResult[i].getValue(columns[15]) != '- None -' && objResult[i].getValue(columns[15]) != 'NaN' && objResult[i].getValue(columns[15]) != 'undefined')
                arrAuxiliar[0] = objResult[i].getValue(columns[15]);
              else
                arrAuxiliar[0] = '';
              //1. denominacion 8 digitos
              var descripcion8digitos = puc8DigDescript[arrAuxiliar[0]];
              if (descripcion8digitos != null && descripcion8digitos != '- None -' && descripcion8digitos != 'NaN' && descripcion8digitos != 'undefined')
                arrAuxiliar[1] = ValidarAcentos(descripcion8digitos);
              else
                arrAuxiliar[1] = '';
              //2. cuenta 6 digitos
              if (objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '- None -' && objResult[i].getValue(columns[0]) != 'NaN' && objResult[i].getValue(columns[0]) != 'undefined')
                arrAuxiliar[2] = objResult[i].getValue(columns[0]);
              else
                arrAuxiliar[2] = '';
              //3. denominacion 6 digitos
              if (objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '- None -' && objResult[i].getValue(columns[1]) != 'NaN' && objResult[i].getValue(columns[1]) != 'undefined')
                arrAuxiliar[3] = ValidarAcentos(objResult[i].getValue(columns[1]));
              else
                arrAuxiliar[3] = '';
              //4.  sum debitos
              if (objResult[i].getValue(columns[2]) != null && objResult[i].getValue(columns[2]) != '- None -' && objResult[i].getValue(columns[2]) != 'NaN' && objResult[i].getValue(columns[2]) != 'undefined')
                arrAuxiliar[4] = objResult[i].getValue(columns[2]);
              else
                arrAuxiliar[4] = 0.0;
              //5.  sum credito
              if (objResult[i].getValue(columns[3]) != null && objResult[i].getValue(columns[3]) != '- None -' && objResult[i].getValue(columns[3]) != 'NaN' && objResult[i].getValue(columns[3]) != 'undefined')
                arrAuxiliar[5] = objResult[i].getValue(columns[3]);
              else
                arrAuxiliar[5] = 0.0;
              //6.  cuenta 4 digitos
              if (objResult[i].getValue(columns[4]) != null && objResult[i].getValue(columns[4]) != '- None -' && objResult[i].getValue(columns[4]) != 'NaN' && objResult[i].getValue(columns[4]) != 'undefined')
                arrAuxiliar[6] = objResult[i].getValue(columns[4]);
              else
                arrAuxiliar[6] = '';
              //7.  denominacion 4 digitos
              if (objResult[i].getValue(columns[5]) != null && objResult[i].getValue(columns[5]) != '- None -' && objResult[i].getValue(columns[5]) != 'NaN' && objResult[i].getValue(columns[5]) != 'undefined')
                arrAuxiliar[7] = ValidarAcentos(objResult[i].getValue(columns[5]));
              else
                arrAuxiliar[7] = '';
              //8.  cuenta 2 digitos
              if (objResult[i].getValue(columns[6]) != null && objResult[i].getValue(columns[6]) != '- None -' && objResult[i].getValue(columns[6]) != 'NaN' && objResult[i].getValue(columns[6]) != 'undefined')
                arrAuxiliar[8] = objResult[i].getValue(columns[6]);
              else
                arrAuxiliar[8] = '';
              //9.  denominacion 2 digitos
              if (objResult[i].getValue(columns[7]) != null && objResult[i].getValue(columns[7]) != '- None -' && objResult[i].getValue(columns[7]) != 'NaN' && objResult[i].getValue(columns[7]) != 'undefined')
                arrAuxiliar[9] = ValidarAcentos(objResult[i].getValue(columns[7]));
              else
                arrAuxiliar[9] = '';
              //10.  cuenta 1 digito
              if (objResult[i].getValue(columns[8]) != null && objResult[i].getValue(columns[8]) != '- None -' && objResult[i].getValue(columns[8]) != 'NaN' && objResult[i].getValue(columns[8]) != 'undefined')
                arrAuxiliar[10] = objResult[i].getValue(columns[8]);
              else
                arrAuxiliar[10] = '';
              //11.  denominacion 1 digito
              if (objResult[i].getValue(columns[9]) != null && objResult[i].getValue(columns[9]) != '- None -' && objResult[i].getValue(columns[9]) != 'NaN' && objResult[i].getValue(columns[9]) != 'undefined')
                arrAuxiliar[11] = ValidarAcentos(objResult[i].getValue(columns[9]));
              else
                arrAuxiliar[11] = '';
              //12. saldo
              if (objResult[i].getValue(columns[10]) != null && objResult[i].getValue(columns[10]) != '- None -' && objResult[i].getValue(columns[10]) != 'NaN' && objResult[i].getValue(columns[10]) != 'undefined')
                arrAuxiliar[12] = objResult[i].getValue(columns[10]);
              else
                arrAuxiliar[12] = 0.0;
              //13. tipo cuenta
              if (objResult[i].getValue(columns[11]) != null && objResult[i].getValue(columns[11]) != '- None -' && objResult[i].getValue(columns[11]) != 'NaN' && objResult[i].getValue(columns[11]) != 'undefined')
                arrAuxiliar[13] = ValidarAcentos(objResult[i].getText(columns[11]));
              else
                arrAuxiliar[13] = '';
              //14. number
              if (objResult[i].getValue(columns[12]) != null && objResult[i].getValue(columns[12]) != '- None -' && objResult[i].getValue(columns[12]) != 'NaN' && objResult[i].getValue(columns[12]) != 'undefined')
                arrAuxiliar[14] = objResult[i].getValue(columns[12]);
              else
                arrAuxiliar[14] = '';

              //15
              var val_trainid = objResult[i].getValue(columns[13]) != null && objResult[i].getValue(columns[13]) != '- None -' && objResult[i].getValue(columns[13]) != 'NaN' && objResult[i].getValue(columns[13]) != 'undefined';

              var val_type = objResult[i].getValue(columns[14]) != null && objResult[i].getValue(columns[14]) != '- None -' && objResult[i].getValue(columns[14]) != 'NaN' && objResult[i].getValue(columns[14]) != 'undefined';

              if (val_type)
                arrAuxiliar[15] = objResult[i].getText(columns[14]);
              else
                arrAuxiliar[15] = '';

              if (val_trainid)
                arrAuxiliar[15] += " - " + objResult[i].getValue(columns[13]);
            } else {
              //0. cuenta 6 digitos
              if (objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '- None -' && objResult[i].getValue(columns[0]) != 'NaN' && objResult[i].getValue(columns[0]) != 'undefined')
                arrAuxiliar[0] = objResult[i].getValue(columns[0]);
              else
                arrAuxiliar[0] = '';
              //1. denominacion 6 digitos
              if (objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '- None -' && objResult[i].getValue(columns[1]) != 'NaN' && objResult[i].getValue(columns[1]) != 'undefined')
                arrAuxiliar[1] = ValidarAcentos(objResult[i].getValue(columns[1]));
              else
                arrAuxiliar[1] = '';
              //2.  sum debitos
              if (objResult[i].getValue(columns[2]) != null && objResult[i].getValue(columns[2]) != '- None -' && objResult[i].getValue(columns[2]) != 'NaN' && objResult[i].getValue(columns[2]) != 'undefined')
                arrAuxiliar[2] = objResult[i].getValue(columns[2]);
              else
                arrAuxiliar[2] = 0.0;
              //3.  sum credito
              if (objResult[i].getValue(columns[3]) != null && objResult[i].getValue(columns[3]) != '- None -' && objResult[i].getValue(columns[3]) != 'NaN' && objResult[i].getValue(columns[3]) != 'undefined')
                arrAuxiliar[3] = objResult[i].getValue(columns[3]);
              else
                arrAuxiliar[3] = 0.0;
              //4.  cuenta 4 digitos
              if (objResult[i].getValue(columns[4]) != null && objResult[i].getValue(columns[4]) != '- None -' && objResult[i].getValue(columns[4]) != 'NaN' && objResult[i].getValue(columns[4]) != 'undefined')
                arrAuxiliar[4] = objResult[i].getValue(columns[4]);
              else
                arrAuxiliar[4] = '';
              //5.  denominacion 4 digitos
              if (objResult[i].getValue(columns[5]) != null && objResult[i].getValue(columns[5]) != '- None -' && objResult[i].getValue(columns[5]) != 'NaN' && objResult[i].getValue(columns[5]) != 'undefined')
                arrAuxiliar[5] = ValidarAcentos(objResult[i].getValue(columns[5]));
              else
                arrAuxiliar[5] = '';
              //6.  cuenta 2 digitos
              if (objResult[i].getValue(columns[6]) != null && objResult[i].getValue(columns[6]) != '- None -' && objResult[i].getValue(columns[6]) != 'NaN' && objResult[i].getValue(columns[6]) != 'undefined')
                arrAuxiliar[6] = objResult[i].getValue(columns[6]);
              else
                arrAuxiliar[6] = '';
              //7.  denominacion 2 digitos
              if (objResult[i].getValue(columns[7]) != null && objResult[i].getValue(columns[7]) != '- None -' && objResult[i].getValue(columns[7]) != 'NaN' && objResult[i].getValue(columns[7]) != 'undefined')
                arrAuxiliar[7] = ValidarAcentos(objResult[i].getValue(columns[7]));
              else
                arrAuxiliar[7] = '';
              //8.  cuenta 1 digito
              if (objResult[i].getValue(columns[8]) != null && objResult[i].getValue(columns[8]) != '- None -' && objResult[i].getValue(columns[8]) != 'NaN' && objResult[i].getValue(columns[8]) != 'undefined')
                arrAuxiliar[8] = objResult[i].getValue(columns[8]);
              else
                arrAuxiliar[8] = '';
              //9.  denominacion 1 digito
              if (objResult[i].getValue(columns[9]) != null && objResult[i].getValue(columns[9]) != '- None -' && objResult[i].getValue(columns[9]) != 'NaN' && objResult[i].getValue(columns[9]) != 'undefined')
                arrAuxiliar[9] = ValidarAcentos(objResult[i].getValue(columns[9]));
              else
                arrAuxiliar[9] = '';
              //10. saldo
              if (objResult[i].getValue(columns[10]) != null && objResult[i].getValue(columns[10]) != '- None -' && objResult[i].getValue(columns[10]) != 'NaN' && objResult[i].getValue(columns[10]) != 'undefined')
                arrAuxiliar[10] = objResult[i].getValue(columns[10]);
              else
                arrAuxiliar[10] = 0.0;
              //11. tipo cuenta
              if (objResult[i].getValue(columns[11]) != null && objResult[i].getValue(columns[11]) != '- None -' && objResult[i].getValue(columns[11]) != 'NaN' && objResult[i].getValue(columns[11]) != 'undefined')
                arrAuxiliar[11] = ValidarAcentos(objResult[i].getText(columns[11]));
              else
                arrAuxiliar[11] = '';
              //12. number
              if (objResult[i].getValue(columns[12]) != null && objResult[i].getValue(columns[12]) != '- None -' && objResult[i].getValue(columns[12]) != 'NaN' && objResult[i].getValue(columns[12]) != 'undefined')
                arrAuxiliar[12] = objResult[i].getValue(columns[12]);
              else
                arrAuxiliar[12] = '';

              //13
              var val_trainid = objResult[i].getValue(columns[13]) != null && objResult[i].getValue(columns[13]) != '- None -' && objResult[i].getValue(columns[13]) != 'NaN' && objResult[i].getValue(columns[13]) != 'undefined';

              var val_type = objResult[i].getValue(columns[14]) != null && objResult[i].getValue(columns[14]) != '- None -' && objResult[i].getValue(columns[14]) != 'NaN' && objResult[i].getValue(columns[14]) != 'undefined';

              if (val_type)
                arrAuxiliar[13] = objResult[i].getText(columns[14]);
              else
                arrAuxiliar[13] = '';

              if (val_trainid)
                arrAuxiliar[13] += " - " + objResult[i].getValue(columns[13]);
            }

            if (!type && otherType == 2) {
              var tempPuc = arrAuxiliar[0].substring(0, 1);
              if (tempPuc != 4 && tempPuc != 5 && tempPuc != 6 && tempPuc != 7) {
                ArrMontosAux.push(arrAuxiliar);
              }
            } else if (type && otherType == 1) {
              ArrMontosAux.push(arrAuxiliar);
            } else if (!type && otherType == 3) {
              var tempPuc = arrAuxiliar[0].substring(0, 1);
              if (tempPuc == 4 || tempPuc == 5 || tempPuc == 6 || tempPuc == 7) {
                ArrMontosAux.push(arrAuxiliar);
              }
            }
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

      return ArrMontosAux;
    }

    function ObtenerParametros() {

      paramperiodo = objContext.getParameter({
        name: 'custscript_lmry_co_maybaldet_period'
      });
      paramsubsidi = objContext.getParameter({
        name: 'custscript_lmry_co_maybaldet_subsidiary'
      });
      paramidlog = objContext.getParameter({
        name: 'custscript_lmry_co_maybaldet_idlog'
      });
      paramMultibook = objContext.getParameter({
        name: 'custscript_lmry_co_maybaldet_multibook'
      });
      paramBucle = objContext.getParameter({
        name: 'custscript_lmry_co_maybaldet_bucle'
      });
      paramAdjustment = objContext.getParameter({
        name: 'custscript_lmry_co_maybaldet_adjust'
      });

      paramOchoDigitos = objContext.getParameter({
        name: 'custscript_lmry_co_maybaldet_digits'
      });

      if (paramBucle == null || paramBucle == '') {
        paramBucle = 0;
      }
      log.debug('[ObtenerParametros] Parametros', paramperiodo + ' - ' + paramsubsidi + ' - ' + paramidlog + ' - ' + paramMultibook + ' - ' + paramBucle + ' - ' + paramAdjustment + ' - ' + paramOchoDigitos);

      // Datos de la empresa
      var configpage = config.load({
        type: 'companyinformation'
      });
      companyruc = configpage.getValue('employerid');
      companyname = configpage.getValue('legalname');

      if (featuresubs == true) {
        if (featureMultipCalendars || featureMultipCalendars == 'T') {
          var subsidyName = search.lookupFields({
            type: search.Type.SUBSIDIARY,
            id: paramsubsidi,
            columns: ['legalname', 'taxidnum', 'fiscalcalendar', 'taxfiscalcalendar']
          });
          //NO SE VALIDA EL CAMPO FISCAL/TAX CALENDAR PORQUE ES OBLIGATORIO EN LA SUBSIDIARIA
          calendarSubsi = {
            id: subsidyName.fiscalcalendar[0].value,
            nombre: subsidyName.fiscalcalendar[0].text
          }
          calendarSubsi = JSON.stringify(calendarSubsi);

          taxCalendarSubsi = {
            id: subsidyName.taxfiscalcalendar[0].value,
            nombre: subsidyName.taxfiscalcalendar[0].text
          }
          taxCalendarSubsi = JSON.stringify(taxCalendarSubsi);
        }

        companyname = ObtainNameSubsidiaria(paramsubsidi);
        companyruc = ObtainFederalIdSubsidiaria(paramsubsidi);

        //Validacion de feature Special Accounting Period
        var licenses = libraryRPT.getLicenses(paramsubsidi);
        featAccountingSpecial = libraryRPT.getAuthorization(677, licenses); //true o false

      }

      if (feamultibook || feamultibook == 'T') {
        multibook_name = search.lookupFields({
          type: search.Type.ACCOUNTING_BOOK,
          id: paramMultibook,
          columns: ['name']
        });
      }

      /* DATOS DE PERIODO */
      var columnFrom = search.lookupFields({
        type: 'accountingperiod',
        id: paramperiodo,
        columns: ['enddate', 'periodname', 'startdate']
      });

      periodstartdate = columnFrom.startdate;
      periodenddate = columnFrom.enddate;
      periodname = columnFrom.periodname;

      if (featAccountingSpecial || featAccountingSpecial == true) {
        var periodos = obtenerPeriodosEspeciales(paramperiodo);
        var arrayPeriods = periodos.split(",");

        periodstartdate = arrayPeriods[0];
        periodenddate = arrayPeriods[1];
        periodstartdateSpecial = arrayPeriods[0];
        periodenddateSpecial = arrayPeriods[1];
      }


      var tempdate = format.parse({
        value: periodstartdate,
        type: format.Type.DATE
      });

      monthStartD = tempdate.getMonth() + 1;

      if (('' + monthStartD).length == 1) {
        monthStartD = '0' + monthStartD;
      } else {
        monthStartD = monthStartD + '';
      }

      yearStartD = tempdate.getFullYear();

      // VALIDAR QUE EL RECORD TENGA AL MENOS 1 PUC DE 8 DIGITOS
      var congfiguradoRecord8digits = configuracion8PUCs();
      if (paramOchoDigitos == 'T') {
        if (!congfiguradoRecord8digits) {
          noData(GLOBAL_LABELS["Alert12"][language]);
          error8digitos = true;
        }
      }
    }

    function configuracion8PUCs() {
      var configuracionPuc = search.create({
        type: "customrecord_lmry_co_puc",
        filters: [
          ["isinactive", "is", "F"]
        ],
        columns: [
          search.createColumn({
            name: "name",
            sort: search.Sort.ASC,
            label: "0. PUC 8D"
          })
        ]
      });
      var Length8Digitos = search.createFilter({
        name: 'formulatext',
        formula: 'LENGTH({name})',
        operator: search.Operator.IS,
        values: 8
      });
      configuracionPuc.filters.push(Length8Digitos);
      var savedSearch = configuracionPuc.run();
      var objResult = savedSearch.getRange(0, 1000);
      var cantidadSearch = objResult.length;
      if (cantidadSearch > 0) {
        return true;
      } else {
        return false;
      }
    }


    function Reporte8digitos() {
      var valido8digitos = '';
      var intDMinReg = 0;
      var intDMaxReg = intDMinReg + 1000;
      if (feamultibook == true) {
        var savedsearch = search.load({
          /*Latamready - CO Ledger and balance Multibook*/
          type: 'accountingtransaction',
          id: 'customsearch_lmry_co_mayor_bal_multi',
        })
        savedsearch.filters.push(search.createFilter({
          name: "formulatext",
          formula: "LENGTH({account.custrecord_lmry_co_puc_id})",
          operator: "is",
          values: 8
        }));
        var searchresult = savedsearch.run();
        var objResult = searchresult.getRange(intDMinReg, intDMaxReg);
        var intLength = objResult.length; // busqueda8digitos
        if (intLength > 0) {
          //log.debug('Entro aca', 'Entro a que SI tiene multi y SI tiene al menos 1 columna de 8 digitos');
          valido8digitos = 'T';
        } else {
          //log.debug('Entro aca', 'Entro a que SI tiene multi y NO tiene al menos 1 columna de 8 digitos');
          valido8digitos = 'F';
        }
      } else {
        var savedsearch = search.load({
          /*Latamready - CO Ledger and balance*/
          id: 'customsearch_lmry_co_mayor_balance_trans'
        });
        savedsearch.filters.push(search.createFilter({
          name: "formulatext",
          formula: "LENGTH({account.custrecord_lmry_co_puc_id})",
          operator: "is",
          values: 8
        }));
        var searchresult = savedsearch.run();
        var objResult = searchresult.getRange(intDMinReg, intDMaxReg);
        var intLength = objResult.length;
        if (intLength > 0) {
          //log.debug('Entro aca', 'Entro a que NO tiene multi y SI tiene al menos 1 columna de 8 digitos');
          valido8digitos = 'T';
        } else {
          //log.debug('Entro aca', 'Entro a que NO tiene multi y NO tiene al menos 1 columna de 8 digitos');
          valido8digitos = 'F';
        }
      }
      return valido8digitos;
    }




    function AgruparCuentas(ArrAux) {
      var ArrReturn = new Array();

      if (ArrAux.length != 0) {
        var account = null;
        var document = null;
        var index = -1;

        for (var i = 0; i < ArrAux.length; i++) {

          if (paramOchoDigitos == 'T') {
            account = ArrAux[i][0];
            document = ArrAux[i][15];
            index = findTransaction(ArrReturn, account, document);
            if (index == -1) {
              var aux = new Array();
              aux[0] = ArrAux[i][0];
              aux[1] = ArrAux[i][1];
              aux[2] = ArrAux[i][2];
              aux[3] = ArrAux[i][3];
              aux[4] = Number(ArrAux[i][4]);// sumDebitos
              aux[5] = Number(ArrAux[i][5]);// sumCreditos
              aux[6] = ArrAux[i][6];
              aux[7] = ArrAux[i][7];
              aux[8] = ArrAux[i][8];
              aux[9] = ArrAux[i][9];
              aux[10] = ArrAux[i][10];
              aux[11] = ArrAux[i][11];
              aux[12] = Number(ArrAux[i][12]);// sumSaldo
              aux[13] = ArrAux[i][13];
              aux[14] = ArrAux[i][14];
              aux[15] = ArrAux[i][15];
              ArrReturn.push(aux);
            } else {
              ArrReturn[index][4] += Number(ArrAux[i][4]);// sumDebitos
              ArrReturn[index][5] += Number(ArrAux[i][5]);// sumCreditos
              ArrReturn[index][12] += Number(ArrAux[i][12]);// sumSaldo
            }
          } else {
            account = ArrAux[i][0];
            document = ArrAux[i][13];
            index = findTransaction(ArrReturn, account, document);
            if (index == -1) {
              var aux = new Array();
              aux[0] = ArrAux[i][0];
              aux[1] = ArrAux[i][1];
              aux[2] = Number(ArrAux[i][2]);// sumDebitos
              aux[3] = Number(ArrAux[i][3]);// sumCreditos
              aux[4] = ArrAux[i][4];
              aux[5] = ArrAux[i][5];
              aux[6] = ArrAux[i][6];
              aux[7] = ArrAux[i][7];
              aux[8] = ArrAux[i][8];
              aux[9] = ArrAux[i][9];
              aux[10] = Number(ArrAux[i][10]);// sumSaldo
              aux[11] = ArrAux[i][11];
              aux[12] = ArrAux[i][12];
              aux[13] = ArrAux[i][13];
              ArrReturn.push(aux);

            } else {
              ArrReturn[index][2] += Number(ArrAux[i][2]);// sumDebitos
              ArrReturn[index][3] += Number(ArrAux[i][3]);// sumCreditos
              ArrReturn[index][10] += Number(ArrAux[i][10]);// sumSaldo
            }
          }
        }
        return ArrReturn;
      }
    }

    function findTransaction(arrTransactions, account, document) {
      var index = -1;
      for (var i = 0; i < arrTransactions.length; i++) {
        if (paramOchoDigitos == 'T') {
          if (account == arrTransactions[i][0] && document == arrTransactions[i][15]) {
            index = i;
            break;
          }
        } else {
          if (account == arrTransactions[i][0] && document == arrTransactions[i][13]) {
            index = i;
            break;
          }
        }



      }
      return index;
    }

    function agregarArregloSeisDigitos() {
      // apenas recibo el arreglo reordenar para que este de esa forma
      ordenarArregloOchoDigitos(ArrMontosFinal);
      //log.debug('ArrMontosFinal despues de ordenar para agrupar a 6 digitos', ArrMontosFinal);
      /*
       * ARRAY FINAL
       * 0. cuenta
       * 1. denominacion
       * 2. debitos antes
       * 3. creditos antes
       * 4. debitos actual
       * 5. creditos actual
       * 6. nuevo saldo debitos
       * 7. nuevo saldo creditos
       * 8. cuenta 6 digitos
       * 9. denominacion 6 digitos
       * 10. cuenta 4 digitos
       * 11. denominacion 4 digitos
       * 12. cuenta 2 digitos
       * 13. denominacion 2 digitos
       * 14. cuenta 1 digito
       * 15. denominacion 1 digito
       */
      var cuenta_aux = ArrMontosFinal[0][8];
      var array_6_digitos = new Array();
      array_6_digitos[0] = cuenta_aux;
      array_6_digitos[1] = ArrMontosFinal[0][9];
      array_6_digitos[2] = 0.0;
      array_6_digitos[3] = 0.0;
      array_6_digitos[4] = 0.0;
      array_6_digitos[5] = 0.0;
      array_6_digitos[6] = 0.0;
      array_6_digitos[7] = 0.0;
      array_6_digitos[8] = ArrMontosFinal[0][8];
      array_6_digitos[9] = ArrMontosFinal[0][9];
      array_6_digitos[10] = ArrMontosFinal[0][10];
      array_6_digitos[11] = ArrMontosFinal[0][11];
      array_6_digitos[12] = ArrMontosFinal[0][12];
      array_6_digitos[13] = ArrMontosFinal[0][13];
      array_6_digitos[14] = ArrMontosFinal[0][14];
      array_6_digitos[15] = ArrMontosFinal[0][15];
      array_6_digitos[16] = ArrMontosFinal[0][16];
      array_6_digitos[17] = ArrMontosFinal[0][17];
      array_6_digitos[18] = ArrMontosFinal[0][18];
      array_6_digitos[19] = '';

      //Agregar al inicio del array
      ArrMontosFinal.splice(0, 0, array_6_digitos);
      var array_cuentas = new Array();
      array_cuentas[0] = array_6_digitos;
      var cont = 1;
      for (var i = 0; i < ArrMontosFinal.length; i++) {
        if (ArrMontosFinal[i][8] != cuenta_aux) {
          cuenta_aux = ArrMontosFinal[i][8];
          var array_aux = new Array();
          array_aux[0] = cuenta_aux;
          array_aux[1] = ArrMontosFinal[i][9];
          array_aux[2] = 0.0;
          array_aux[3] = 0.0;
          array_aux[4] = 0.0;
          array_aux[5] = 0.0;
          array_aux[6] = 0.0;
          array_aux[7] = 0.0;
          array_aux[8] = ArrMontosFinal[i][8];
          array_aux[9] = ArrMontosFinal[i][9];
          array_aux[10] = ArrMontosFinal[i][10];
          array_aux[11] = ArrMontosFinal[i][11];
          array_aux[12] = ArrMontosFinal[i][12];
          array_aux[13] = ArrMontosFinal[i][13];
          array_aux[14] = ArrMontosFinal[i][14];
          array_aux[15] = ArrMontosFinal[i][15];
          array_aux[16] = ArrMontosFinal[i][16];
          array_aux[17] = ArrMontosFinal[i][17];
          array_aux[18] = ArrMontosFinal[i][18];
          array_aux[19] = '';

          array_cuentas[cont] = array_aux;
          cont++;
          ArrMontosFinal.splice(i, 0, array_aux);
        }
      }
      //calcular montos de cuentas
      for (var i = 0; i < array_cuentas.length; i++) {
        for (var j = 0; j < ArrMontosFinal.length; j++) {
          if (array_cuentas[i][0] == ArrMontosFinal[j][8] && ArrMontosFinal[j][0].length == 8) {
            array_cuentas[i][2] += Number(ArrMontosFinal[j][2]);
            array_cuentas[i][3] += Number(ArrMontosFinal[j][3]);
            array_cuentas[i][4] += Number(ArrMontosFinal[j][4]);
            array_cuentas[i][5] += Number(ArrMontosFinal[j][5]);
            array_cuentas[i][6] += Number(ArrMontosFinal[j][6]);
            array_cuentas[i][7] += Number(ArrMontosFinal[j][7]);
          }
        }
      }
      //reemplazar array vacio en el ArrMontosFinal
      for (var i = 0; i < array_cuentas.length; i++) {
        for (var j = 0; j < ArrMontosFinal.length; j++) {
          if (array_cuentas[i][0] == ArrMontosFinal[j][0]) {
            ArrMontosFinal[j] = array_cuentas[i];
          }
        }
      }

      agregarArregloCuatroDigitos();
    }

    function ordenarArregloOchoDigitos(array) {
      for (var i = 0; i < array.length; i++) {
        var seisDigit = array[i][2];
        var seisDescription = array[i][3];
        for (var j = 2; j < 10; j++) {
          if (j >= 2 && j <= 7) {
            array[i][j] = array[i][j + 2];
          } else {
            array[i][8] = seisDigit;
            array[i][9] = seisDescription;
          }
        }
      }
    }

    function agregarArregloCuatroDigitos() {
      var array_4_digitos = new Array();
      //log.debug('dentro de la funcion agregarArregloCuatroDigitos ArrMontosFinal', ArrMontosFinal);
      var cuenta_aux = ''

      if (paramOchoDigitos == 'T') {
        /*
        * ARRAY FINAL
        * 0. cuenta
        * 1. denominacion
        * 2. debitos antes
        * 3. creditos antes
        * 4. debitos actual
        * 5. creditos actual
        * 6. nuevo saldo debitos
        * 7. nuevo saldo creditos
        * 8. cuenta 6 digitos
        * 9. denominacion 6 digitos
        * 10. cuenta 4 digitos
        * 11. denominacion 4 digitos
        * 12. cuenta 2 digitos
        * 13. denominacion 2 digitos
        * 14. cuenta 1 digito
        * 15. denominacion 1 digito
        */
        cuenta_aux = ArrMontosFinal[0][10];

        array_4_digitos[0] = cuenta_aux;
        array_4_digitos[1] = ArrMontosFinal[0][11];
        array_4_digitos[2] = 0.0;
        array_4_digitos[3] = 0.0;
        array_4_digitos[4] = 0.0;
        array_4_digitos[5] = 0.0;
        array_4_digitos[6] = 0.0;
        array_4_digitos[7] = 0.0;
        array_4_digitos[8] = ArrMontosFinal[0][8];
        array_4_digitos[9] = ArrMontosFinal[0][9];
        array_4_digitos[10] = ArrMontosFinal[0][10];
        array_4_digitos[11] = ArrMontosFinal[0][11];
        array_4_digitos[12] = ArrMontosFinal[0][12];
        array_4_digitos[13] = ArrMontosFinal[0][13];
        array_4_digitos[14] = ArrMontosFinal[0][14];
        array_4_digitos[15] = ArrMontosFinal[0][15];
        array_4_digitos[16] = ArrMontosFinal[0][16];
        array_4_digitos[17] = ArrMontosFinal[0][17];
        array_4_digitos[18] = ArrMontosFinal[0][18];
        array_4_digitos[19] = '';//ArrMontosFinal[0][19];

      } else {
        /*
        * ARRAY FINAL
        * 0. cuenta
        * 1. denominacion
        * 2. debitos antes
        * 3. creditos antes
        * 4. debitos actual
        * 5. creditos actual
        * 6. nuevo saldo debitos
        * 7. nuevo saldo creditos
        * 8. cuenta 4 digitos
        * 9. denominacion 4 digitos
        * 10. cuenta 2 digitos
        * 11. denominacion 2 digitos
        * 12. cuenta 1 digito
        * 13. denominacion 1 digito
        */

        cuenta_aux = ArrMontosFinal[0][8];

        array_4_digitos[0] = cuenta_aux;
        array_4_digitos[1] = ArrMontosFinal[0][9];
        array_4_digitos[2] = 0.0;
        array_4_digitos[3] = 0.0;
        array_4_digitos[4] = 0.0;
        array_4_digitos[5] = 0.0;
        array_4_digitos[6] = 0.0;
        array_4_digitos[7] = 0.0;
        array_4_digitos[8] = ArrMontosFinal[0][8];
        array_4_digitos[9] = ArrMontosFinal[0][9];
        array_4_digitos[10] = ArrMontosFinal[0][10];
        array_4_digitos[11] = ArrMontosFinal[0][11];
        array_4_digitos[12] = ArrMontosFinal[0][12];
        array_4_digitos[13] = ArrMontosFinal[0][13];
        array_4_digitos[14] = ArrMontosFinal[0][14];
        array_4_digitos[15] = ArrMontosFinal[0][15];
        array_4_digitos[16] = ArrMontosFinal[0][16];
        array_4_digitos[17] = '';//ArrMontosFinal[0][17];
      }


      //Agregar al inicio del array
      ArrMontosFinal.splice(0, 0, array_4_digitos);

      var array_cuentas = new Array();
      var accounts = new Array();
      array_cuentas[0] = array_4_digitos;
      accounts[0] = array_4_digitos[0];
      var cont = 1;
      for (var i = 0; i < ArrMontosFinal.length; i++) {

        if (paramOchoDigitos == 'T') {

          if (accounts.indexOf(ArrMontosFinal[i][10]) == -1) {
            cuenta_aux = ArrMontosFinal[i][10];
            var array_aux = new Array();

            array_aux[0] = cuenta_aux;
            array_aux[1] = ArrMontosFinal[i][11];
            array_aux[2] = 0.0;
            array_aux[3] = 0.0;
            array_aux[4] = 0.0;
            array_aux[5] = 0.0;
            array_aux[6] = 0.0;
            array_aux[7] = 0.0;
            array_aux[8] = ArrMontosFinal[i][8];
            array_aux[9] = ArrMontosFinal[i][9];
            array_aux[10] = ArrMontosFinal[i][10];
            array_aux[11] = ArrMontosFinal[i][11];
            array_aux[12] = ArrMontosFinal[i][12];
            array_aux[13] = ArrMontosFinal[i][13];
            array_aux[14] = ArrMontosFinal[i][14];
            array_aux[15] = ArrMontosFinal[i][15];
            array_aux[16] = ArrMontosFinal[i][16];
            array_aux[17] = ArrMontosFinal[i][17];
            array_aux[18] = ArrMontosFinal[i][18];
            array_aux[19] = '';//ArrMontosFinal[i][19];
            array_cuentas[cont] = array_aux;
            accounts[cont] = cuenta_aux;
            cont++;
            ArrMontosFinal.splice(i, 0, array_aux);
          }
        } else {
          if (accounts.indexOf(ArrMontosFinal[i][8]) == -1) {
            cuenta_aux = ArrMontosFinal[i][8];
            var array_aux = new Array();

            array_aux[0] = cuenta_aux;
            array_aux[1] = ArrMontosFinal[i][9];
            array_aux[2] = 0.0;
            array_aux[3] = 0.0;
            array_aux[4] = 0.0;
            array_aux[5] = 0.0;
            array_aux[6] = 0.0;
            array_aux[7] = 0.0;
            array_aux[8] = ArrMontosFinal[i][8];
            array_aux[9] = ArrMontosFinal[i][9];
            array_aux[10] = ArrMontosFinal[i][10];
            array_aux[11] = ArrMontosFinal[i][11];
            array_aux[12] = ArrMontosFinal[i][12];
            array_aux[13] = ArrMontosFinal[i][13];
            array_aux[14] = ArrMontosFinal[i][14];
            array_aux[15] = ArrMontosFinal[i][15];
            array_aux[16] = ArrMontosFinal[i][16];
            array_aux[17] = '';//ArrMontosFinal[i][17];
            array_cuentas[cont] = array_aux;
            accounts[cont] = cuenta_aux;
            cont++;
            ArrMontosFinal.splice(i, 0, array_aux);
          }
        }
      }

      //calcular montos de cuentas
      for (var i = 0; i < array_cuentas.length; i++) {
        for (var j = 0; j < ArrMontosFinal.length; j++) {

          if (paramOchoDigitos == 'T') {
            if (array_cuentas[i][0] == ArrMontosFinal[j][10] && ArrMontosFinal[j][0].length == 8) {
              array_cuentas[i][2] += Number(ArrMontosFinal[j][2]);
              array_cuentas[i][3] += Number(ArrMontosFinal[j][3]);
              array_cuentas[i][4] += Number(ArrMontosFinal[j][4]);
              array_cuentas[i][5] += Number(ArrMontosFinal[j][5]);
              array_cuentas[i][6] += Number(ArrMontosFinal[j][6]);
              array_cuentas[i][7] += Number(ArrMontosFinal[j][7]);
            }
          } else {
            if (array_cuentas[i][0] == ArrMontosFinal[j][8] && ArrMontosFinal[j][0].length == 6) {
              array_cuentas[i][2] += Number(ArrMontosFinal[j][2]);
              array_cuentas[i][3] += Number(ArrMontosFinal[j][3]);
              array_cuentas[i][4] += Number(ArrMontosFinal[j][4]);
              array_cuentas[i][5] += Number(ArrMontosFinal[j][5]);
              array_cuentas[i][6] += Number(ArrMontosFinal[j][6]);
              array_cuentas[i][7] += Number(ArrMontosFinal[j][7]);
            }
          }


        }
      }

      //reemplazar array vacio en el ArrMontosFinal
      for (var i = 0; i < array_cuentas.length; i++) {
        for (var j = 0; j < ArrMontosFinal.length; j++) {
          if (array_cuentas[i][0] == ArrMontosFinal[j][0]) {
            ArrMontosFinal[j] = array_cuentas[i];
          }
        }
      }

      agregarArregloDosDigitos();

    }

    function agregarArregloDosDigitos() {
      //log.debug('dentro de la funcion agregarArregloDosDigitos ArrMontosFinal', ArrMontosFinal);
      var grupo_aux = "";
      var array_aux_uno = new Array();
      if (paramOchoDigitos == 'T') {
        /*
        * ARRAY SEARCH
        * 0. cuenta 4 digitos
        * 1. denominacion 4 digitos
        * 2. documento
        * 3. sum debitos
        * 4. sum credito
        * 5. cuenta 6 digitos
        * 6. denominacion 6 digitos
        * 7. cuenta 4 digitos
        * 8. denominacion 4 digitos
        * 9. cuenta 2 digitos
        * 10. denominacion 2 digitos
        * 11. cuenta 1 digito
        * 12. denominacion 1 digito
        * 13. period id
        * 14. period name
        * 15. saldo
        */

        grupo_aux = ArrMontosFinal[0][12];

        array_aux_uno[0] = grupo_aux;
        array_aux_uno[1] = ArrMontosFinal[0][13];
        array_aux_uno[2] = 0;
        array_aux_uno[3] = 0;
        array_aux_uno[4] = 0;
        array_aux_uno[5] = 0;
        array_aux_uno[6] = 0;
        array_aux_uno[7] = 0;
        array_aux_uno[8] = ArrMontosFinal[0][8];
        array_aux_uno[9] = ArrMontosFinal[0][9];
        array_aux_uno[10] = ArrMontosFinal[0][10];
        array_aux_uno[11] = ArrMontosFinal[0][11];
        array_aux_uno[12] = ArrMontosFinal[0][12];
        array_aux_uno[13] = ArrMontosFinal[0][13];
        array_aux_uno[14] = ArrMontosFinal[0][14];
        array_aux_uno[15] = ArrMontosFinal[0][15];
        array_aux_uno[16] = ArrMontosFinal[0][16];
        array_aux_uno[17] = ArrMontosFinal[0][17];
        array_aux_uno[18] = ArrMontosFinal[0][18];
        array_aux_uno[19] = '';//ArrMontosFinal[0][19];

      } else {
        /*
        * ARRAY SEARCH
        * 0. cuenta 6 digitos
        * 1. denominacion 6 digitos
        * 2. documento
        * 3. sum debitos
        * 4. sum credito
        * 5. cuenta 4 digitos
        * 6. denominacion 4 digitos
        * 7. cuenta 2 digitos
        * 8. denominacion 2 digitos
        * 9. cuenta 1 digito
        * 10. denominacion 1 digito
        * 11. period id
        * 12. period name
        * 13. saldo
        */

        grupo_aux = ArrMontosFinal[0][10];

        array_aux_uno[0] = grupo_aux;
        array_aux_uno[1] = ArrMontosFinal[0][11];
        array_aux_uno[2] = 0;
        array_aux_uno[3] = 0;
        array_aux_uno[4] = 0;
        array_aux_uno[5] = 0;
        array_aux_uno[6] = 0;
        array_aux_uno[7] = 0;
        array_aux_uno[8] = ArrMontosFinal[0][8];
        array_aux_uno[9] = ArrMontosFinal[0][9];
        array_aux_uno[10] = ArrMontosFinal[0][10];
        array_aux_uno[11] = ArrMontosFinal[0][11];
        array_aux_uno[12] = ArrMontosFinal[0][12];
        array_aux_uno[13] = ArrMontosFinal[0][13];
        array_aux_uno[14] = ArrMontosFinal[0][14];
        array_aux_uno[15] = ArrMontosFinal[0][15];
        array_aux_uno[16] = ArrMontosFinal[0][16];
        array_aux_uno[17] = '';//ArrMontosFinal[0][17];
      }



      ArrMontosFinal.splice(0, 0, array_aux_uno);

      var array_cuentas = new Array();
      var accounts = new Array();
      array_cuentas[0] = array_aux_uno;
      accounts[0] = array_aux_uno[0];
      var cont = 1;

      //quiebre de grupo
      for (var i = 0; i < ArrMontosFinal.length; i++) {

        if (paramOchoDigitos == 'T') {
          if (accounts.indexOf(ArrMontosFinal[i][12]) == -1) {
            grupo_aux = ArrMontosFinal[i][12];
            var array_aux = new Array();
            /* 0 -> cuenta
             * 1 -> denominacion
             * 2 -> debe antes
             * 3 -> haber antes
             * 4 -> debe actual
             * 5 -> haber actual
             * 6 -> debe nuevo saldo
             * 7 -> haber nuevo saldo
             */
            array_aux[0] = grupo_aux;
            array_aux[1] = ArrMontosFinal[i][13];
            array_aux[2] = 0.0;
            array_aux[3] = 0.0;
            array_aux[4] = 0.0;
            array_aux[5] = 0.0;
            array_aux[6] = 0.0;
            array_aux[7] = 0.0;
            array_aux[8] = ArrMontosFinal[i][8];
            array_aux[9] = ArrMontosFinal[i][9];
            array_aux[10] = ArrMontosFinal[i][10];
            array_aux[11] = ArrMontosFinal[i][11];
            array_aux[12] = ArrMontosFinal[i][12];
            array_aux[13] = ArrMontosFinal[i][13];
            array_aux[14] = ArrMontosFinal[i][14];
            array_aux[15] = ArrMontosFinal[i][15];
            array_aux[16] = ArrMontosFinal[i][16];
            array_aux[17] = ArrMontosFinal[i][17];
            array_aux[18] = ArrMontosFinal[i][18];
            array_aux[19] = '';//ArrMontosFinal[i][19];
            array_cuentas[cont] = array_aux;
            accounts[cont] = array_aux[0];
            cont++;
            ArrMontosFinal.splice(i, 0, array_aux);
          }
        } else {
          if (accounts.indexOf(ArrMontosFinal[i][10]) == -1) {
            grupo_aux = ArrMontosFinal[i][10];
            var array_aux = new Array();
            /* 0 -> cuenta
             * 1 -> denominacion
             * 2 -> debe antes
             * 3 -> haber antes
             * 4 -> debe actual
             * 5 -> haber actual
             * 6 -> debe nuevo saldo
             * 7 -> haber nuevo saldo
             */
            array_aux[0] = grupo_aux;
            array_aux[1] = ArrMontosFinal[i][11];
            array_aux[2] = 0.0;
            array_aux[3] = 0.0;
            array_aux[4] = 0.0;
            array_aux[5] = 0.0;
            array_aux[6] = 0.0;
            array_aux[7] = 0.0;
            array_aux[8] = ArrMontosFinal[i][8];
            array_aux[9] = ArrMontosFinal[i][9];
            array_aux[10] = ArrMontosFinal[i][10];
            array_aux[11] = ArrMontosFinal[i][11];
            array_aux[12] = ArrMontosFinal[i][12];
            array_aux[13] = ArrMontosFinal[i][13];
            array_aux[14] = ArrMontosFinal[i][14];
            array_aux[15] = ArrMontosFinal[i][15];
            array_aux[16] = ArrMontosFinal[i][16];
            array_aux[17] = '';//ArrMontosFinal[i][17];
            array_cuentas[cont] = array_aux;
            accounts[cont] = array_aux[0];
            cont++;
            ArrMontosFinal.splice(i, 0, array_aux);
          }
        }

      }

      //calcular montos de cuentas
      for (var i = 0; i < array_cuentas.length; i++) {
        for (var j = 0; j < ArrMontosFinal.length; j++) {

          if (paramOchoDigitos == 'T') {
            if (array_cuentas[i][0] == ArrMontosFinal[j][12] && ArrMontosFinal[j][0].length == 8) {
              array_cuentas[i][2] += Number(ArrMontosFinal[j][2]);
              array_cuentas[i][3] += Number(ArrMontosFinal[j][3]);
              array_cuentas[i][4] += Number(ArrMontosFinal[j][4]);
              array_cuentas[i][5] += Number(ArrMontosFinal[j][5]);
              array_cuentas[i][6] += Number(ArrMontosFinal[j][6]);
              array_cuentas[i][7] += Number(ArrMontosFinal[j][7]);
            }
          } else {
            if (array_cuentas[i][0] == ArrMontosFinal[j][10] && ArrMontosFinal[j][0].length == 6) {
              array_cuentas[i][2] += Number(ArrMontosFinal[j][2]);
              array_cuentas[i][3] += Number(ArrMontosFinal[j][3]);
              array_cuentas[i][4] += Number(ArrMontosFinal[j][4]);
              array_cuentas[i][5] += Number(ArrMontosFinal[j][5]);
              array_cuentas[i][6] += Number(ArrMontosFinal[j][6]);
              array_cuentas[i][7] += Number(ArrMontosFinal[j][7]);
            }
          }
        }
      }

      //reemplazar array vacio del ArrMontosFinal
      for (var i = 0; i < array_cuentas.length; i++) {
        for (var j = 0; j < ArrMontosFinal.length; j++) {
          if (array_cuentas[i][0] == ArrMontosFinal[j][0]) {
            ArrMontosFinal[j] = array_cuentas[i];
          }
        }
      }
      agregarArregloUnDigito();
    }

    function agregarArregloUnDigito() {
      //log.debug('dentro de la funcion agregarArregloUnDigito ArrMontosFinal', ArrMontosFinal);
      var clase_aux = "";
      var array_aux_uno = new Array();

      if (paramOchoDigitos == 'T') {
        clase_aux = ArrMontosFinal[0][14];

        array_aux_uno[0] = clase_aux;
        array_aux_uno[1] = ArrMontosFinal[0][15];
        array_aux_uno[2] = 0.0;
        array_aux_uno[3] = 0.0;
        array_aux_uno[4] = 0.0;
        array_aux_uno[5] = 0.0;
        array_aux_uno[6] = 0.0;
        array_aux_uno[7] = 0.0;
        array_aux_uno[8] = ArrMontosFinal[0][8];
        array_aux_uno[9] = ArrMontosFinal[0][9];
        array_aux_uno[10] = ArrMontosFinal[0][10];
        array_aux_uno[11] = ArrMontosFinal[0][11];
        array_aux_uno[12] = ArrMontosFinal[0][12];
        array_aux_uno[13] = ArrMontosFinal[0][13];
        array_aux_uno[14] = ArrMontosFinal[0][14];
        array_aux_uno[15] = ArrMontosFinal[0][15];
        array_aux_uno[16] = ArrMontosFinal[0][16];
        array_aux_uno[17] = ArrMontosFinal[0][17];
        array_aux_uno[18] = ArrMontosFinal[0][18];
        array_aux_uno[19] = '';//ArrMontosFinal[0][19];
      } else {
        clase_aux = ArrMontosFinal[0][12];

        array_aux_uno[0] = clase_aux;
        array_aux_uno[1] = ArrMontosFinal[0][13];
        array_aux_uno[2] = 0.0;
        array_aux_uno[3] = 0.0;
        array_aux_uno[4] = 0.0;
        array_aux_uno[5] = 0.0;
        array_aux_uno[6] = 0.0;
        array_aux_uno[7] = 0.0;
        array_aux_uno[8] = ArrMontosFinal[0][8];
        array_aux_uno[9] = ArrMontosFinal[0][9];
        array_aux_uno[10] = ArrMontosFinal[0][10];
        array_aux_uno[11] = ArrMontosFinal[0][11];
        array_aux_uno[12] = ArrMontosFinal[0][12];
        array_aux_uno[13] = ArrMontosFinal[0][13];
        array_aux_uno[14] = ArrMontosFinal[0][14];
        array_aux_uno[15] = ArrMontosFinal[0][15];
        array_aux_uno[16] = ArrMontosFinal[0][16];
        array_aux_uno[17] = '';//ArrMontosFinal[0][17];
      }



      ArrMontosFinal.splice(0, 0, array_aux_uno);

      var array_cuentas = new Array();
      var accounts = new Array();
      array_cuentas[0] = array_aux_uno;
      accounts[0] = array_aux_uno[0];
      var cont = 1;

      //quiebre de grupo
      for (var i = 0; i < ArrMontosFinal.length; i++) {

        if (paramOchoDigitos == 'T') {
          if (accounts.indexOf(ArrMontosFinal[i][14]) == -1) {
            clase_aux = ArrMontosFinal[i][14];
            var array_aux = new Array();
            /* 0 -> cuenta
             * 1 -> denominacion
             * 2 -> debe antes
             * 3 -> haber antes
             * 4 -> debe actual
             * 5 -> haber actual
             * 6 -> debe nuevo saldo
             * 7 -> haber nuevo saldo
             */
            array_aux[0] = clase_aux;
            array_aux[1] = ArrMontosFinal[i][15];
            array_aux[2] = 0.0;
            array_aux[3] = 0.0;
            array_aux[4] = 0.0;
            array_aux[5] = 0.0;
            array_aux[6] = 0.0;
            array_aux[7] = 0.0;
            array_aux[8] = ArrMontosFinal[i][8];
            array_aux[9] = ArrMontosFinal[i][9];
            array_aux[10] = ArrMontosFinal[i][10];
            array_aux[11] = ArrMontosFinal[i][11];
            array_aux[12] = ArrMontosFinal[i][12];
            array_aux[13] = ArrMontosFinal[i][13];
            array_aux[14] = ArrMontosFinal[i][14];
            array_aux[15] = ArrMontosFinal[i][15];
            array_aux[16] = ArrMontosFinal[i][16];
            array_aux[17] = ArrMontosFinal[i][17];
            array_aux[18] = ArrMontosFinal[i][18];
            array_aux[19] = '';//ArrMontosFinal[i][19];
            array_cuentas[cont] = array_aux;
            accounts[cont] = array_aux[0];
            cont++;
            ArrMontosFinal.splice(i, 0, array_aux);
          }
        } else {
          if (accounts.indexOf(ArrMontosFinal[i][12]) == -1) {
            clase_aux = ArrMontosFinal[i][12];
            var array_aux = new Array();
            /* 0 -> cuenta
             * 1 -> denominacion
             * 2 -> debe antes
             * 3 -> haber antes
             * 4 -> debe actual
             * 5 -> haber actual
             * 6 -> debe nuevo saldo
             * 7 -> haber nuevo saldo
             */
            array_aux[0] = clase_aux;
            array_aux[1] = ArrMontosFinal[i][13];
            array_aux[2] = 0.0;
            array_aux[3] = 0.0;
            array_aux[4] = 0.0;
            array_aux[5] = 0.0;
            array_aux[6] = 0.0;
            array_aux[7] = 0.0;
            array_aux[8] = ArrMontosFinal[i][8];
            array_aux[9] = ArrMontosFinal[i][9];
            array_aux[10] = ArrMontosFinal[i][10];
            array_aux[11] = ArrMontosFinal[i][11];
            array_aux[12] = ArrMontosFinal[i][12];
            array_aux[13] = ArrMontosFinal[i][13];
            array_aux[14] = ArrMontosFinal[i][14];
            array_aux[15] = ArrMontosFinal[i][15];
            array_aux[16] = ArrMontosFinal[i][16];
            array_aux[17] = '';//ArrMontosFinal[i][17];
            array_cuentas[cont] = array_aux;
            accounts[cont] = array_aux[0];
            cont++;
            ArrMontosFinal.splice(i, 0, array_aux);
          }
        }
      }

      //calcular montos de cuentas
      for (var i = 0; i < array_cuentas.length; i++) {
        for (var j = 0; j < ArrMontosFinal.length; j++) {

          if (paramOchoDigitos == 'T') {
            if (array_cuentas[i][0] == ArrMontosFinal[j][14] && ArrMontosFinal[j][0].length == 8) {
              array_cuentas[i][2] += Number(ArrMontosFinal[j][2]);
              array_cuentas[i][3] += Number(ArrMontosFinal[j][3]);
              array_cuentas[i][4] += Number(ArrMontosFinal[j][4]);
              array_cuentas[i][5] += Number(ArrMontosFinal[j][5]);
              array_cuentas[i][6] += Number(ArrMontosFinal[j][6]);
              array_cuentas[i][7] += Number(ArrMontosFinal[j][7]);
            }
          } else {
            if (array_cuentas[i][0] == ArrMontosFinal[j][12] && ArrMontosFinal[j][0].length == 6) {
              array_cuentas[i][2] += Number(ArrMontosFinal[j][2]);
              array_cuentas[i][3] += Number(ArrMontosFinal[j][3]);
              array_cuentas[i][4] += Number(ArrMontosFinal[j][4]);
              array_cuentas[i][5] += Number(ArrMontosFinal[j][5]);
              array_cuentas[i][6] += Number(ArrMontosFinal[j][6]);
              array_cuentas[i][7] += Number(ArrMontosFinal[j][7]);
            }
          }
        }
      }
      //reemplazar array vacio del ArrMontosFinal
      for (var i = 0; i < array_cuentas.length; i++) {
        for (var j = 0; j < ArrMontosFinal.length; j++) {
          if (array_cuentas[i][0] == ArrMontosFinal[j][0]) {
            ArrMontosFinal[j] = array_cuentas[i];
          }
        }
      }

    }

    function orderBalance() {
      ArrMontosFinal.forEach(function balance(transaction) {

        if (paramOchoDigitos == 'T') {
          var lengthAccount = transaction[0].length;
          if (lengthAccount != 8) {
            var currentDedit = transaction[6];
            var currentCredit = transaction[7];
            if (currentDedit != 0 && currentCredit != 0) {
              var balance = currentDedit + currentCredit;
              if (balance < 0) {
                transaction[6] = 0.0;
                transaction[7] = balance;
              } else if (balance > 0) {
                transaction[6] = balance;
                transaction[7] = 0.0;
              } else if (balance == 0) {
                transaction[6] = 0.0;
                transaction[7] = 0.0;
              }
            }
          }
        } else {
          var lengthAccount = transaction[0].length;
          if (lengthAccount != 6) {
            var currentDedit = transaction[4];
            var currentCredit = transaction[5];
            if (currentDedit != 0 && currentCredit != 0) {
              var balance = currentDedit + currentCredit;
              if (balance < 0) {
                transaction[4] = 0.0;
                transaction[5] = balance;
              } else if (balance > 0) {
                transaction[4] = balance;
                transaction[5] = 0.0;
              } else if (balance == 0) {
                transaction[4] = 0.0;
                transaction[5] = 0.0;
              }
            }
          }
        }


      });

    }

    function OrdenarArreglo(arrTemporal) {

      arrTemporal.sort(sortFunction);

      function sortFunction(a, b) {

        if (a[0] === b[0]) {
          return 0;
        } else {
          return (a[0] < b[0]) ? -1 : 1;
        }
      }

      return arrTemporal;
    }

    function obtenerArregloFinalSeisDigitos() {
      /*
       * SEARCH
       * 0. cuenta
       * 1. denominacion
       * 2. sum debitos
       * 3. sum credito
       * 4. cuenta 4 digitos
       * 5. denominacion 4 digitos
       * 6. cuenta 2 digitos
       * 7. denominacion 2 digitos
       * 8. cuenta 1 digito
       * 9. denominacion 1 digito
       * 10.period id
       * 11.period name
       * 12.saldo
       */

      var arr_final = new Array();
      var cont = 0;
      //log.debug('ArrMontosAntes dentro de obtenerArregloFinalSeisDigitos', ArrMontosAntes);
      //log.debug('ArrMontosActual dentro de obtenerArregloFinalSeisDigitos', ArrMontosActual);
      if (ArrMontosActual != null && ArrMontosActual.length != 0) {
        for (var i = 0; i < ArrMontosActual.length; i++) {
          var sub = new Array();

          sub[0] = ArrMontosActual[i][0];
          sub[1] = ArrMontosActual[i][1];
          sub[2] = 0.0;
          sub[3] = 0.0;
          sub[4] = 0.0;//ArrMontosActual[i][2];
          sub[5] = 0.0;//ArrMontosActual[i][3];
          if (ArrMontosActual[i][10] < 0) {
            sub[6] = 0.0;
            sub[7] = ArrMontosActual[i][10];
            sub[4] = 0.0;
            sub[5] = ArrMontosActual[i][10];
          } else if (ArrMontosActual[i][10] > 0) {
            sub[6] = ArrMontosActual[i][10];
            sub[7] = 0.0;
            sub[4] = ArrMontosActual[i][10];
            sub[5] = 0.0;
          } else if (ArrMontosActual[i][10] == 0.0) {
            sub[6] = 0.0;
            sub[7] = 0.0;
            sub[4] = 0.0;
            sub[5] = 0.0;
          }
          sub[8] = ArrMontosActual[i][4];
          sub[9] = ArrMontosActual[i][5];
          sub[10] = ArrMontosActual[i][6];
          sub[11] = ArrMontosActual[i][7];
          sub[12] = ArrMontosActual[i][8];
          sub[13] = ArrMontosActual[i][9];
          sub[14] = ArrMontosActual[i][10];
          sub[15] = ArrMontosActual[i][11];
          sub[16] = ArrMontosActual[i][12];
          sub[17] = ArrMontosActual[i][13];

          //  arr_final[cont] = sub;
          //  cont++;
          if (ArrMontosActual[i][2] != 0 || ArrMontosActual[i][3] != 0) {
            arr_final[cont] = sub;
            cont++;
          }
        }
      }
      if (ArrMontosAntes != null && ArrMontosAntes.length != 0) {
        for (var i = 0; i < ArrMontosAntes.length; i++) {
          var sub_array_2 = new Array();
          sub_array_2[0] = ArrMontosAntes[i][0];
          sub_array_2[1] = ArrMontosAntes[i][1];
          sub_array_2[2] = 0.0;
          sub_array_2[3] = 0.0;
          sub_array_2[4] = 0.0;
          sub_array_2[5] = 0.0;

          if (ArrMontosAntes[i][10] < 0) {
            sub_array_2[6] = 0.0;
            sub_array_2[7] = ArrMontosAntes[i][10];
            sub_array_2[2] = 0.0;
            sub_array_2[3] = ArrMontosAntes[i][10];
          } else if (ArrMontosAntes[i][10] > 0) {
            sub_array_2[6] = ArrMontosAntes[i][10];
            sub_array_2[7] = 0.0;
            sub_array_2[2] = ArrMontosAntes[i][10];
            sub_array_2[3] = 0.0;
          } else if (ArrMontosAntes[i][10] == 0) {
            sub_array_2[6] = 0.0;
            sub_array_2[7] = 0.0;
            sub_array_2[2] = 0.0;
            sub_array_2[3] = 0.0;
          }
          sub_array_2[8] = ArrMontosAntes[i][4];
          sub_array_2[9] = ArrMontosAntes[i][5];
          sub_array_2[10] = ArrMontosAntes[i][6];
          sub_array_2[11] = ArrMontosAntes[i][7];
          sub_array_2[12] = ArrMontosAntes[i][8];
          sub_array_2[13] = ArrMontosAntes[i][9];
          sub_array_2[14] = ArrMontosAntes[i][10];
          sub_array_2[15] = ArrMontosAntes[i][11];
          sub_array_2[16] = ArrMontosAntes[i][12];
          sub_array_2[17] = ArrMontosAntes[i][13];

          if (ArrMontosAntes[i][2] != 0 || ArrMontosAntes[i][3] != 0) {
            arr_final[cont] = sub_array_2;
            cont++;
          }
        }
      }

      if (arr_final.length == 0) {
        flagEmpty = true;
      }


      return arr_final;
    }

    function obtenerArregloFinalOchoDigitos() {
      /*
       * SEARCH
        * 0. cuenta
        * 1. denominacion
        * 2. cuenta 6 digitos
        * 3. denominacion 6 digitos
        * 4. sum debitos
        * 5. sum credito
        * 6. cuenta 4 digitos
        * 7. denominacion 4 digitos
        * 8. cuenta 2 digitos
        * 9. denominacion 2 digitos
        * 10. cuenta 1 digito
        * 11. denominacion 1 digito
        * 12.period id
        * 13.period name
        * 14.saldo
       */

      var arr_final = new Array();
      var cont = 0;

      //log.debug('ArrMontosAntes dentro de obtenerArregloFinalSeisDigitos', ArrMontosAntes);
      //log.debug('ArrMontosActual dentro de obtenerArregloFinalSeisDigitos', ArrMontosActual);

      if (ArrMontosActual != null && ArrMontosActual.length != 0) {
        for (var i = 0; i < ArrMontosActual.length; i++) {
          var sub = new Array();


          sub[0] = ArrMontosActual[i][0];
          sub[1] = ArrMontosActual[i][1];

          sub[2] = ArrMontosActual[i][2];
          sub[3] = ArrMontosActual[i][3];
          sub[4] = 0.0;
          sub[5] = 0.0;
          sub[6] = 0.0;//ArrMontosActual[i][2];
          sub[7] = 0.0;//ArrMontosActual[i][3];
          if (ArrMontosActual[i][12] < 0) {
            sub[8] = 0.0;
            sub[9] = ArrMontosActual[i][12];
            sub[6] = 0.0;
            sub[7] = ArrMontosActual[i][12];
          } else if (ArrMontosActual[i][12] > 0) {
            sub[8] = ArrMontosActual[i][12];
            sub[9] = 0.0;
            sub[6] = ArrMontosActual[i][12];
            sub[7] = 0.0;
          } else if (ArrMontosActual[i][12] == 0.0) {
            sub[8] = 0.0;
            sub[9] = 0.0;
            sub[6] = 0.0;
            sub[7] = 0.0;
          }
          sub[10] = ArrMontosActual[i][6];
          sub[11] = ArrMontosActual[i][7];
          sub[12] = ArrMontosActual[i][8];
          sub[13] = ArrMontosActual[i][9];
          sub[14] = ArrMontosActual[i][10];
          sub[15] = ArrMontosActual[i][11];
          sub[16] = ArrMontosActual[i][12];
          sub[17] = ArrMontosActual[i][13];
          sub[18] = ArrMontosActual[i][14];
          sub[19] = ArrMontosActual[i][15];

          //  arr_final[cont] = sub;
          //  cont++;
          if (ArrMontosActual[i][4] != 0 || ArrMontosActual[i][5] != 0) {
            arr_final[cont] = sub;
            cont++;
          }
        }
      }
      if (ArrMontosAntes != null && ArrMontosAntes.length != 0) {
        for (var i = 0; i < ArrMontosAntes.length; i++) {
          var sub_array_2 = new Array();
          sub_array_2[0] = ArrMontosAntes[i][0];
          sub_array_2[1] = ArrMontosAntes[i][1];
          sub_array_2[2] = ArrMontosAntes[i][2];
          sub_array_2[3] = ArrMontosAntes[i][3];
          sub_array_2[4] = 0.0;
          sub_array_2[5] = 0.0;
          sub_array_2[6] = 0.0;
          sub_array_2[7] = 0.0;

          if (ArrMontosAntes[i][12] < 0) {
            sub_array_2[8] = 0.0;
            sub_array_2[9] = ArrMontosAntes[i][12];
            sub_array_2[4] = 0.0;
            sub_array_2[5] = ArrMontosAntes[i][12];
          } else if (ArrMontosAntes[i][12] > 0) {
            sub_array_2[8] = ArrMontosAntes[i][12];
            sub_array_2[9] = 0.0;
            sub_array_2[4] = ArrMontosAntes[i][12];
            sub_array_2[5] = 0.0;
          } else if (ArrMontosAntes[i][12] == 0) {
            sub_array_2[8] = 0.0;
            sub_array_2[9] = 0.0;
            sub_array_2[4] = 0.0;
            sub_array_2[5] = 0.0;
          }
          sub_array_2[10] = ArrMontosAntes[i][6];
          sub_array_2[11] = ArrMontosAntes[i][7];
          sub_array_2[12] = ArrMontosAntes[i][8];
          sub_array_2[13] = ArrMontosAntes[i][9];
          sub_array_2[14] = ArrMontosAntes[i][10];
          sub_array_2[15] = ArrMontosAntes[i][11];
          sub_array_2[16] = ArrMontosAntes[i][12];
          sub_array_2[17] = ArrMontosAntes[i][13];
          sub_array_2[18] = ArrMontosAntes[i][14];
          sub_array_2[19] = ArrMontosAntes[i][15];

          if (ArrMontosAntes[i][4] != 0 || ArrMontosAntes[i][5] != 0) {
            arr_final[cont] = sub_array_2;
            cont++;
          }
        }
      }

      if (arr_final.length == 0) {
        flagEmpty = true;
      }

      return arr_final;
    }


    function ObtenerFormulaFechas() {
      var formula = 'CASE WHEN ';
      for (var i = 0; i < AccountingPeriodsArray.length; i++) {
        formula += "{transaction.postingperiod.id} = '" + AccountingPeriodsArray[i][3] + "'";
        if (i != AccountingPeriodsArray.length - 1) {
          formula += ' OR ';
        }
      }
      formula += ' THEN 1 ELSE 0 END';
      return formula;
    }

    function ObtenerFechas(periodStartDate) {
      var intDMinReg = 0;
      var intDMaxReg = 1000;
      var _cont = 0;

      var filters = new Array();
      filters[0] = search.createFilter({
        name: 'isyear',
        operator: search.Operator.IS,
        values: 'F'
      });
      filters[1] = search.createFilter({
        name: 'isquarter',
        operator: search.Operator.IS,
        values: 'F'
      });
      filters[2] = search.createFilter({
        name: 'startdate',
        operator: search.Operator.BEFORE,
        values: [periodStartDate]
      });

      var columns = new Array();
      columns[0] = search.createColumn({
        name: 'periodname',
        summary: 'group',
      });
      columns[1] = search.createColumn({
        name: 'startdate',
        summary: 'group',
        sort: search.Sort.ASC
      });
      columns[2] = search.createColumn({
        name: 'enddate',
        summary: 'group',
      });
      columns[3] = search.createColumn({
        name: 'internalid',
        summary: 'group',
      });

      var savedsearch = search.create({
        type: 'accountingperiod',
        filters: filters,
        columns: columns
      });

      var objResult = savedsearch.run().getRange(intDMinReg, intDMaxReg);

      if (objResult != null) {
        for (var i = 0; i < objResult.length; i++) {
          columns = objResult[i].columns;
          rowArray = new Array();
          // Period Name
          rowArray[0] = objResult[i].getValue(columns[0]);
          // StartDate
          rowArray[1] = objResult[i].getValue(columns[1]);
          // EndDate
          rowArray[2] = objResult[i].getValue(columns[2]);
          // Internal ID
          rowArray[3] = objResult[i].getValue(columns[3]);

          AccountingPeriodsArray[_cont] = rowArray;
          _cont++;
        }
      }
    }

    function ObtieneLibroMayor(type, condAdjust) {
      // Seteo de Porcentaje completo
      objContext.percentComplete = 0.00;
      // Control de Memoria
      var intDMinReg = paramBucle * 1000;
      var intDMaxReg = intDMinReg + 1000;
      // Exedio las unidades
      var DbolStop = false;

      var ArrMontosAux = new Array();

      var savedsearch = search.load({
        /*Latamready - CO Ledger and balance*/
        id: 'customsearch_lmry_co_mayor_balance_trans'
      });

      if ((feamultibook || feamultibook == 'T') && (paramMultibook != '' || paramMultibook != null)) {
        savedsearch.filters.push(search.createFilter({
          name: 'accountingbook',
          join: 'accountingtransaction',
          operator: search.Operator.ANYOF,
          values: [paramMultibook]
        }));
        savedsearch.filters.push(search.createFilter({
          name: 'bookspecifictransaction',
          operator: search.Operator.IS,
          values: 'F'
        }));
        savedsearch.columns.push(search.createColumn({
          name: "formulacurrency",
          summary: "SUM",
          formula: "{accountingtransaction.debitamount}",
          label: "Formula (Currency)"
        }));
        savedsearch.columns.push(search.createColumn({
          name: "formulacurrency",
          summary: "SUM",
          formula: "{accountingtransaction.creditamount}",
          label: "Formula (Currency)"
        }));
        savedsearch.columns.push(search.createColumn({
          name: 'account',
          join: 'accountingtransaction',
          summary: 'GROUP'
        }));

      }

      savedsearch.columns.push(search.createColumn({
        name: 'formulatext',
        formula: 'NVL({tranid},{transactionnumber})',
        summary: 'GROUP',
        label: 'Tran Id or transaction number'
      }));
      savedsearch.columns.push(search.createColumn({
        name: 'type',
        summary: 'GROUP',
        label: 'type'
      }));


      // Valida si es OneWorld
      if (featuresubs) {
        savedsearch.filters.push(search.createFilter({
          name: 'subsidiary',
          operator: search.Operator.IS,
          values: [paramsubsidi]
        }));
      }

      if (type) {
        //para Movimientos
        //Para adjust sin marcar
        if (!condAdjust) {

          var filtroPosting = search.createFilter({
            name: 'postingperiod',
            operator: search.Operator.IS,
            values: [paramperiodo] //317
          })
          savedsearch.filters.push(filtroPosting);
          savedsearch.filters.push(search.createFilter({
            name: 'isadjust',
            join: 'accountingperiod',
            operator: search.Operator.IS,
            values: false
          }));
          //Adjust TRUE
        } else {
          savedsearch.filters.push(search.createFilter({
            name: 'postingperiod',
            operator: search.Operator.IS,
            values: [periodoAdjust] //317
          }));

          if (featurePeriodEnd) {
            var confiPeriodEnd = search.createSetting({
              name: 'includeperiodendtransactions',
              value: 'TRUE'
            })
            savedsearch.settings.push(confiPeriodEnd);
          }
        }

      } else {
        //para saldo anterior
        savedsearch.filters.push(search.createFilter({
          name: 'startdate',
          join: 'accountingperiod',
          operator: search.Operator.BEFORE,
          values: [periodstartdate]
        }));

      }

      if (paramOchoDigitos == 'T') {
        savedsearch.filters.push(search.createFilter({
          name: "formulatext",
          formula: "LENGTH({account.custrecord_lmry_co_puc_id})",
          operator: "is",
          values: 8
        }));

        savedsearch.columns.push(search.createColumn({
          name: "formulatext",
          summary: "GROUP",
          formula: "{account.custrecord_lmry_co_puc_id}",
          sort: search.Sort.ASC,
          label: "PUC 8D ID"
        }));
      }



      var searchresult = savedsearch.run();

      while (!DbolStop) {
        var objResult = searchresult.getRange(intDMinReg, intDMaxReg);

        if (objResult != null) {
          var intLength = objResult.length;
          //  log.debug('Cantidad de lineas',objResult.length);
          for (var i = 0; i < intLength; i++) {
            var columns = objResult[i].columns;
            var arrAuxiliar = new Array();

            if (paramOchoDigitos == 'T') {

              /*
              * 0. cuenta 8 digitos
              * 1. denominacion 8 digito
              * 2.  cuenta 6 digitos
              * 3.  denominacion 6 digitos
              * 4.  sum debitos
              * 5.  sum credito
              * 6.  cuenta 4 digitos
              * 7.  denominacion 4 digitos
              * 8.  cuenta 2 digitos
              * 9.  denominacion 2 digitos
              * 10.  cuenta 1 digito
              * 11.  denominacion 1 digito
              * 12. period id
              * 13. period name
              * 14. saldo
              * 15. TIPO D CUENTA
              * 16.number
              */

              //0. cuenta 8 digitos
              if (objResult[i].getValue(columns[17]) != null && objResult[i].getValue(columns[17]) != '- None -' && objResult[i].getValue(columns[17]) != 'NaN' && objResult[i].getValue(columns[17]) != 'undefined')
                arrAuxiliar[0] = objResult[i].getValue(columns[17]);
              else
                arrAuxiliar[0] = '';
              //1. denominacion 8 digitos
              var descripcion8digitos = puc8DigDescript[arrAuxiliar[0]];

              if (descripcion8digitos != null && descripcion8digitos != '- None -' && descripcion8digitos != 'NaN' && descripcion8digitos != 'undefined')
                arrAuxiliar[1] = ValidarAcentos(descripcion8digitos);
              else
                arrAuxiliar[1] = '';


              //2. cuenta 6 digitos
              if (objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '- None -' && objResult[i].getValue(columns[0]) != 'NaN' && objResult[i].getValue(columns[0]) != 'undefined')
                arrAuxiliar[2] = objResult[i].getValue(columns[0]);
              else
                arrAuxiliar[2] = '';
              //3. denominacion 6 digitos
              if (objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '- None -' && objResult[i].getValue(columns[1]) != 'NaN' && objResult[i].getValue(columns[1]) != 'undefined')
                arrAuxiliar[3] = ValidarAcentos(objResult[i].getValue(columns[1]));
              else
                arrAuxiliar[3] = '';
              //4.  sum debitos
              if (!feamultibook) {
                if (objResult[i].getValue(columns[2]) != null && objResult[i].getValue(columns[2]) != '- None -' && objResult[i].getValue(columns[2]) != 'NaN' && objResult[i].getValue(columns[2]) != 'undefined')
                  arrAuxiliar[4] = objResult[i].getValue(columns[2]);
                else
                  arrAuxiliar[4] = 0.0;
              } else {
                if (objResult[i].getValue(columns[13]) != null && objResult[i].getValue(columns[13]) != '- None -' && objResult[i].getValue(columns[13]) != 'NaN' && objResult[i].getValue(columns[13]) != 'undefined')
                  arrAuxiliar[4] = objResult[i].getValue(columns[13]);
                else
                  arrAuxiliar[4] = 0.0;
              }
              //5.  sum credito
              if (!feamultibook) {
                if (objResult[i].getValue(columns[3]) != null && objResult[i].getValue(columns[3]) != '- None -' && objResult[i].getValue(columns[3]) != 'NaN' && objResult[i].getValue(columns[3]) != 'undefined')
                  arrAuxiliar[5] = objResult[i].getValue(columns[3]);
                else
                  arrAuxiliar[5] = 0.0;
              } else {
                if (objResult[i].getValue(columns[14]) != null && objResult[i].getValue(columns[14]) != '- None -' && objResult[i].getValue(columns[14]) != 'NaN' && objResult[i].getValue(columns[14]) != 'undefined')
                  arrAuxiliar[5] = objResult[i].getValue(columns[14]);
                else
                  arrAuxiliar[5] = 0.0;
              }
              //6.  cuenta 4 digitos
              if (objResult[i].getValue(columns[4]) != null && objResult[i].getValue(columns[4]) != '- None -' && objResult[i].getValue(columns[4]) != 'NaN' && objResult[i].getValue(columns[4]) != 'undefined')
                arrAuxiliar[6] = objResult[i].getValue(columns[4]);
              else
                arrAuxiliar[6] = '';
              //7.  denominacion 4 digitos
              if (objResult[i].getValue(columns[5]) != null && objResult[i].getValue(columns[5]) != '- None -' && objResult[i].getValue(columns[5]) != 'NaN' && objResult[i].getValue(columns[5]) != 'undefined')
                arrAuxiliar[7] = ValidarAcentos(objResult[i].getValue(columns[5]));
              else
                arrAuxiliar[7] = '';
              //8.  cuenta 2 digitos
              if (objResult[i].getValue(columns[6]) != null && objResult[i].getValue(columns[6]) != '- None -' && objResult[i].getValue(columns[6]) != 'NaN' && objResult[i].getValue(columns[6]) != 'undefined')
                arrAuxiliar[8] = objResult[i].getValue(columns[6]);
              else
                arrAuxiliar[8] = '';
              //9.  denominacion 2 digitos
              if (objResult[i].getValue(columns[7]) != null && objResult[i].getValue(columns[7]) != '- None -' && objResult[i].getValue(columns[7]) != 'NaN' && objResult[i].getValue(columns[7]) != 'undefined')
                arrAuxiliar[9] = ValidarAcentos(objResult[i].getValue(columns[7]));
              else
                arrAuxiliar[9] = '';
              //10.  cuenta 1 digito
              if (objResult[i].getValue(columns[8]) != null && objResult[i].getValue(columns[8]) != '- None -' && objResult[i].getValue(columns[8]) != 'NaN' && objResult[i].getValue(columns[8]) != 'undefined')
                arrAuxiliar[10] = objResult[i].getValue(columns[8]);
              else
                arrAuxiliar[10] = '';
              //11.  denominacion 1 digito
              if (objResult[i].getValue(columns[9]) != null && objResult[i].getValue(columns[9]) != '- None -' && objResult[i].getValue(columns[9]) != 'NaN' && objResult[i].getValue(columns[9]) != 'undefined')
                arrAuxiliar[11] = ValidarAcentos(objResult[i].getValue(columns[9]));
              else
                arrAuxiliar[11] = '';
              //12. saldo
              if (!feamultibook) {
                if (objResult[i].getValue(columns[10]) != null && objResult[i].getValue(columns[10]) != '- None -' && objResult[i].getValue(columns[10]) != 'NaN' && objResult[i].getValue(columns[10]) != 'undefined')
                  arrAuxiliar[12] = objResult[i].getValue(columns[10]);
                else
                  arrAuxiliar[12] = 0.0;
              } else {
                arrAuxiliar[12] = arrAuxiliar[4] - arrAuxiliar[5];
              }

              //13. tipo cuenta
              if (objResult[i].getValue(columns[11]) != null && objResult[i].getValue(columns[11]) != '- None -' && objResult[i].getValue(columns[11]) != 'NaN' && objResult[i].getValue(columns[11]) != 'undefined')
                arrAuxiliar[13] = ValidarAcentos(objResult[i].getText(columns[11]));
              else
                arrAuxiliar[13] = '';
              //14. number
              if (feamultibook) {
                if (objResult[i].getValue(columns[15]) != null && objResult[i].getValue(columns[15]) != '- None -' && objResult[i].getValue(columns[15]) != 'NaN' && objResult[i].getValue(columns[15]) != 'undefined')
                  arrAuxiliar[14] = objResult[i].getValue(columns[15]);
                else
                  arrAuxiliar[14] = '';
              } else {
                if (objResult[i].getValue(columns[12]) != null && objResult[i].getValue(columns[12]) != '- None -' && objResult[i].getValue(columns[12]) != 'NaN' && objResult[i].getValue(columns[12]) != 'undefined')
                  arrAuxiliar[14] = objResult[i].getValue(columns[12]);
                else
                  arrAuxiliar[14] = '';
              }

              //15
              if (feamultibook) {
                var val_trainid = objResult[i].getValue(columns[16]) != null && objResult[i].getValue(columns[16]) != '- None -' && objResult[i].getValue(columns[16]) != 'NaN' && objResult[i].getValue(columns[16]) != 'undefined';
                var val_type = objResult[i].getValue(columns[17]) != null && objResult[i].getValue(columns[17]) != '- None -' && objResult[i].getValue(columns[17]) != 'NaN' && objResult[i].getValue(columns[17]) != 'undefined';
                if (val_type)
                  arrAuxiliar[15] = objResult[i].getText(columns[17]);
                else
                  arrAuxiliar[15] = '';

                if (val_trainid)
                  arrAuxiliar[15] += ' - ' + objResult[i].getValue(columns[16]);

              } else {
                var val_trainid = objResult[i].getValue(columns[13]) != null && objResult[i].getValue(columns[13]) != '- None -' && objResult[i].getValue(columns[13]) != 'NaN' && objResult[i].getValue(columns[13]) != 'undefined';

                var val_type = objResult[i].getValue(columns[14]) != null && objResult[i].getValue(columns[14]) != '- None -' && objResult[i].getValue(columns[14]) != 'NaN' && objResult[i].getValue(columns[14]) != 'undefined';

                if (val_type)
                  arrAuxiliar[15] = objResult[i].getText(columns[14]);
                else
                  arrAuxiliar[15] = '';
                if (val_trainid)
                  arrAuxiliar[15] += " - " + objResult[i].getValue(columns[13]);
              }
            } else {
              /*
              * 0.  cuenta 6 digitos
              * 1.  denominacion 6 digitos
              * 2.  sum debitos
              * 3.  sum credito
              * 4.  cuenta 4 digitos
              * 5.  denominacion 4 digitos
              * 6.  cuenta 2 digitos
              * 7.  denominacion 2 digitos
              * 8.  cuenta 1 digito
              * 9.  denominacion 1 digito
              * 10. period id
              * 11. period name
              * 12. saldo
              * 13. TIPO D CUENTA
              * 14.number
              */
              //0. cuenta 6 digitos
              if (objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '- None -' && objResult[i].getValue(columns[0]) != 'NaN' && objResult[i].getValue(columns[0]) != 'undefined')
                arrAuxiliar[0] = objResult[i].getValue(columns[0]);
              else
                arrAuxiliar[0] = '';
              //1. denominacion 6 digitos
              if (objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '- None -' && objResult[i].getValue(columns[1]) != 'NaN' && objResult[i].getValue(columns[1]) != 'undefined')
                arrAuxiliar[1] = ValidarAcentos(objResult[i].getValue(columns[1]));
              else
                arrAuxiliar[1] = '';
              //2.  sum debitos
              if (!feamultibook) {
                if (objResult[i].getValue(columns[2]) != null && objResult[i].getValue(columns[2]) != '- None -' && objResult[i].getValue(columns[2]) != 'NaN' && objResult[i].getValue(columns[2]) != 'undefined')
                  arrAuxiliar[2] = objResult[i].getValue(columns[2]);
                else
                  arrAuxiliar[2] = 0.0;
              } else {
                if (objResult[i].getValue(columns[13]) != null && objResult[i].getValue(columns[13]) != '- None -' && objResult[i].getValue(columns[13]) != 'NaN' && objResult[i].getValue(columns[13]) != 'undefined')
                  arrAuxiliar[2] = objResult[i].getValue(columns[13]);
                else
                  arrAuxiliar[2] = 0.0;
              }
              //3.  sum credito
              if (!feamultibook) {
                if (objResult[i].getValue(columns[3]) != null && objResult[i].getValue(columns[3]) != '- None -' && objResult[i].getValue(columns[3]) != 'NaN' && objResult[i].getValue(columns[3]) != 'undefined')
                  arrAuxiliar[3] = objResult[i].getValue(columns[3]);
                else
                  arrAuxiliar[3] = 0.0;
              } else {
                if (objResult[i].getValue(columns[14]) != null && objResult[i].getValue(columns[14]) != '- None -' && objResult[i].getValue(columns[14]) != 'NaN' && objResult[i].getValue(columns[14]) != 'undefined')
                  arrAuxiliar[3] = objResult[i].getValue(columns[14]);
                else
                  arrAuxiliar[3] = 0.0;
              }
              //4.  cuenta 4 digitos
              if (objResult[i].getValue(columns[4]) != null && objResult[i].getValue(columns[4]) != '- None -' && objResult[i].getValue(columns[4]) != 'NaN' && objResult[i].getValue(columns[4]) != 'undefined')
                arrAuxiliar[4] = objResult[i].getValue(columns[4]);
              else
                arrAuxiliar[4] = '';
              //5.  denominacion 4 digitos
              if (objResult[i].getValue(columns[5]) != null && objResult[i].getValue(columns[5]) != '- None -' && objResult[i].getValue(columns[5]) != 'NaN' && objResult[i].getValue(columns[5]) != 'undefined')
                arrAuxiliar[5] = ValidarAcentos(objResult[i].getValue(columns[5]));
              else
                arrAuxiliar[5] = '';
              //6.  cuenta 2 digitos
              if (objResult[i].getValue(columns[6]) != null && objResult[i].getValue(columns[6]) != '- None -' && objResult[i].getValue(columns[6]) != 'NaN' && objResult[i].getValue(columns[6]) != 'undefined')
                arrAuxiliar[6] = objResult[i].getValue(columns[6]);
              else
                arrAuxiliar[6] = '';
              //7.  denominacion 2 digitos
              if (objResult[i].getValue(columns[7]) != null && objResult[i].getValue(columns[7]) != '- None -' && objResult[i].getValue(columns[7]) != 'NaN' && objResult[i].getValue(columns[7]) != 'undefined')
                arrAuxiliar[7] = ValidarAcentos(objResult[i].getValue(columns[7]));
              else
                arrAuxiliar[7] = '';
              //8.  cuenta 1 digito
              if (objResult[i].getValue(columns[8]) != null && objResult[i].getValue(columns[8]) != '- None -' && objResult[i].getValue(columns[8]) != 'NaN' && objResult[i].getValue(columns[8]) != 'undefined')
                arrAuxiliar[8] = objResult[i].getValue(columns[8]);
              else
                arrAuxiliar[8] = '';
              //9.  denominacion 1 digito
              if (objResult[i].getValue(columns[9]) != null && objResult[i].getValue(columns[9]) != '- None -' && objResult[i].getValue(columns[9]) != 'NaN' && objResult[i].getValue(columns[9]) != 'undefined')
                arrAuxiliar[9] = ValidarAcentos(objResult[i].getValue(columns[9]));
              else
                arrAuxiliar[9] = '';
              //10. saldo
              if (!feamultibook) {
                if (objResult[i].getValue(columns[10]) != null && objResult[i].getValue(columns[10]) != '- None -' && objResult[i].getValue(columns[10]) != 'NaN' && objResult[i].getValue(columns[10]) != 'undefined')
                  arrAuxiliar[10] = objResult[i].getValue(columns[10]);
                else
                  arrAuxiliar[10] = 0.0;
              } else {
                arrAuxiliar[10] = arrAuxiliar[2] - arrAuxiliar[3];
              }

              //11. tipo cuenta
              if (objResult[i].getValue(columns[11]) != null && objResult[i].getValue(columns[11]) != '- None -' && objResult[i].getValue(columns[11]) != 'NaN' && objResult[i].getValue(columns[11]) != 'undefined')
                arrAuxiliar[11] = ValidarAcentos(objResult[i].getText(columns[11]));
              else
                arrAuxiliar[11] = '';
              //12. number
              if (feamultibook) {
                if (objResult[i].getValue(columns[15]) != null && objResult[i].getValue(columns[15]) != '- None -' && objResult[i].getValue(columns[15]) != 'NaN' && objResult[i].getValue(columns[15]) != 'undefined')
                  arrAuxiliar[12] = objResult[i].getValue(columns[15]);
                else
                  arrAuxiliar[12] = '';
              } else {
                if (objResult[i].getValue(columns[12]) != null && objResult[i].getValue(columns[12]) != '- None -' && objResult[i].getValue(columns[12]) != 'NaN' && objResult[i].getValue(columns[12]) != 'undefined')
                  arrAuxiliar[12] = objResult[i].getValue(columns[12]);
                else
                  arrAuxiliar[12] = '';
              }

              //13
              if (feamultibook) {
                var val_trainid = objResult[i].getValue(columns[16]) != null && objResult[i].getValue(columns[16]) != '- None -' && objResult[i].getValue(columns[16]) != 'NaN' && objResult[i].getValue(columns[16]) != 'undefined';
                var val_type = objResult[i].getValue(columns[17]) != null && objResult[i].getValue(columns[17]) != '- None -' && objResult[i].getValue(columns[17]) != 'NaN' && objResult[i].getValue(columns[17]) != 'undefined';
                if (val_type)
                  arrAuxiliar[13] = objResult[i].getText(columns[17]);
                else
                  arrAuxiliar[13] = '';

                if (val_trainid)
                  arrAuxiliar[13] += ' - ' + objResult[i].getValue(columns[16]);

              } else {
                var val_trainid = objResult[i].getValue(columns[13]) != null && objResult[i].getValue(columns[13]) != '- None -' && objResult[i].getValue(columns[13]) != 'NaN' && objResult[i].getValue(columns[13]) != 'undefined';

                var val_type = objResult[i].getValue(columns[14]) != null && objResult[i].getValue(columns[14]) != '- None -' && objResult[i].getValue(columns[14]) != 'NaN' && objResult[i].getValue(columns[14]) != 'undefined';

                if (val_type)
                  arrAuxiliar[13] = objResult[i].getText(columns[14]);
                else
                  arrAuxiliar[13] = '';
                if (val_trainid)
                  arrAuxiliar[13] += " - " + objResult[i].getValue(columns[13]);
              }
            }

            if (!type) {
              if (feamultibook) {
                //Cambios de cuenta multibook para saldos anteriores
                if (paramOchoDigitos == 'T') {
                  var x = jsonIndexCuentas.get(arrAuxiliar[14]);
                  if (x != undefined) {
                    arrAuxiliar[0] = ArrCuentas[x][10];
                    arrAuxiliar[1] = ArrCuentas[x][11];
                    arrAuxiliar[2] = ArrCuentas[x][8];
                    arrAuxiliar[3] = ArrCuentas[x][9];
                    arrAuxiliar[6] = ArrCuentas[x][6];
                    arrAuxiliar[7] = ArrCuentas[x][7];
                    arrAuxiliar[8] = ArrCuentas[x][4];
                    arrAuxiliar[9] = ArrCuentas[x][5];
                    arrAuxiliar[10] = ArrCuentas[x][2];
                    arrAuxiliar[11] = ArrCuentas[x][3];
                  }
                } else {
                  var x = jsonIndexCuentas.get(arrAuxiliar[12]);
                  if (x != undefined) {
                    arrAuxiliar[0] = ArrCuentas[x][8];
                    arrAuxiliar[1] = ArrCuentas[x][9];
                    arrAuxiliar[4] = ArrCuentas[x][6];
                    arrAuxiliar[5] = ArrCuentas[x][7];
                    arrAuxiliar[6] = ArrCuentas[x][4];
                    arrAuxiliar[7] = ArrCuentas[x][5];
                    arrAuxiliar[8] = ArrCuentas[x][2];
                    arrAuxiliar[9] = ArrCuentas[x][3];
                  }
                }
              }

              var tempPuc = arrAuxiliar[0].substring(0, 1);
              if (tempPuc != 4 && tempPuc != 5 && tempPuc != 6 && tempPuc != 7) {
                ArrMontosAux.push(arrAuxiliar);
              }
            } else {
              ArrMontosAux.push(arrAuxiliar);
            }
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

      return ArrMontosAux;
    }

    function ObtieneLMRestantesPUCs4567() {
      // Seteo de Porcentaje completo
      objContext.percentComplete = 0.00;
      // Control de Memoria
      var intDMinReg = paramBucle * 1000;
      var intDMaxReg = intDMinReg + 1000;
      // Exedio las unidades
      var DbolStop = false;

      var ArrMontosAux = new Array();

      var savedsearch = search.load({
        /*Latamready - CO Ledger and balance*/
        id: 'customsearch_lmry_co_mayor_balance_trans'
      });

      if ((feamultibook || feamultibook == 'T') && (paramMultibook != '' || paramMultibook != null)) {
        savedsearch.filters.push(search.createFilter({
          name: 'accountingbook',
          join: 'accountingtransaction',
          operator: search.Operator.ANYOF,
          values: [paramMultibook]
        }));
        savedsearch.filters.push(search.createFilter({
          name: 'bookspecifictransaction',
          operator: search.Operator.IS,
          values: 'F'
        }));
        savedsearch.columns.push(search.createColumn({
          name: "formulacurrency",
          summary: "SUM",
          formula: "{accountingtransaction.debitamount}",
          label: "Formula (Currency)"
        }));
        savedsearch.columns.push(search.createColumn({
          name: "formulacurrency",
          summary: "SUM",
          formula: "{accountingtransaction.creditamount}",
          label: "Formula (Currency)"
        }));
        savedsearch.columns.push(search.createColumn({
          name: 'account',
          join: 'accountingtransaction',
          summary: 'GROUP'
        }));

      }

      savedsearch.columns.push(search.createColumn({
        name: 'formulatext',
        formula: 'NVL({tranid},{transactionnumber})',
        summary: 'GROUP',
        label: 'Tran Id or transaction number'
      }));
      savedsearch.columns.push(search.createColumn({
        name: 'type',
        summary: 'GROUP',
        label: 'type'
      }));


      // Valida si es OneWorld
      if (featuresubs) {
        savedsearch.filters.push(search.createFilter({
          name: 'subsidiary',
          operator: search.Operator.IS,
          values: [paramsubsidi]
        }));
      }

      // Saldo Anterior Restantes para PUCs 4,5,6 y 7 (Cuentas de Resultados)
      var period_year_init = format.parse({
        value: periodstartdate,
        type: format.Type.DATE
      }).getFullYear();
      var januaryFisrt = new Date(period_year_init, 0, 1);
      var format_january_first = format.format({
        value: januaryFisrt,
        type: format.Type.DATE
      });

      //! Obtiene los periodos anteriores, incluidos los periodos de ajuste
      var arrPeriods = getRangeDate(format_january_first, periodstartdate);

      //Si no hay periodos anteiores se regresa un [] como saldos anteriores
      if (arrPeriods.length == 0) {
        return ArrMontosAux;
      }

      var formula = ObtenerFormulaFiltroPeriodo(arrPeriods, 1);
      var periodFilter = search.createFilter({
        name: 'formulatext',
        operator: search.Operator.IS,
        values: '1',
        formula: formula
      });
      savedsearch.filters.push(periodFilter);

      //Consultar eso a Melany luego
      // if (featurePeriodEnd) {
      //     var confiPeriodEnd = search.createSetting({
      //         name: 'includeperiodendtransactions',
      //         value: 'TRUE'
      //     })
      //     savedsearch.settings.push(confiPeriodEnd);
      // }

      if (paramOchoDigitos == 'T') {
        savedsearch.filters.push(search.createFilter({
          name: "formulatext",
          formula: "LENGTH({account.custrecord_lmry_co_puc_id})",
          operator: "is",
          values: 8
        }));

        savedsearch.columns.push(search.createColumn({
          name: "formulatext",
          summary: "GROUP",
          formula: "{account.custrecord_lmry_co_puc_id}",
          sort: search.Sort.ASC,
          label: "PUC 8D ID"
        }));
      }



      var searchresult = savedsearch.run();

      while (!DbolStop) {
        var objResult = searchresult.getRange(intDMinReg, intDMaxReg);

        if (objResult != null) {
          var intLength = objResult.length;
          //  log.debug('Cantidad de lineas',objResult.length);
          for (var i = 0; i < intLength; i++) {
            var columns = objResult[i].columns;
            var arrAuxiliar = new Array();

            if (paramOchoDigitos == 'T') {

              /*
              * 0. cuenta 8 digitos
              * 1. denominacion 8 digito
              * 2.  cuenta 6 digitos
              * 3.  denominacion 6 digitos
              * 4.  sum debitos
              * 5.  sum credito
              * 6.  cuenta 4 digitos
              * 7.  denominacion 4 digitos
              * 8.  cuenta 2 digitos
              * 9.  denominacion 2 digitos
              * 10.  cuenta 1 digito
              * 11.  denominacion 1 digito
              * 12. period id
              * 13. period name
              * 14. saldo
              * 15. TIPO D CUENTA
              * 16.number
              */

              //0. cuenta 8 digitos
              if (objResult[i].getValue(columns[17]) != null && objResult[i].getValue(columns[17]) != '- None -' && objResult[i].getValue(columns[17]) != 'NaN' && objResult[i].getValue(columns[17]) != 'undefined')
                arrAuxiliar[0] = objResult[i].getValue(columns[17]);
              else
                arrAuxiliar[0] = '';
              //1. denominacion 8 digitos
              var descripcion8digitos = puc8DigDescript[arrAuxiliar[0]];

              if (descripcion8digitos != null && descripcion8digitos != '- None -' && descripcion8digitos != 'NaN' && descripcion8digitos != 'undefined')
                arrAuxiliar[1] = ValidarAcentos(descripcion8digitos);
              else
                arrAuxiliar[1] = '';


              //2. cuenta 6 digitos
              if (objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '- None -' && objResult[i].getValue(columns[0]) != 'NaN' && objResult[i].getValue(columns[0]) != 'undefined')
                arrAuxiliar[2] = objResult[i].getValue(columns[0]);
              else
                arrAuxiliar[2] = '';
              //3. denominacion 6 digitos
              if (objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '- None -' && objResult[i].getValue(columns[1]) != 'NaN' && objResult[i].getValue(columns[1]) != 'undefined')
                arrAuxiliar[3] = ValidarAcentos(objResult[i].getValue(columns[1]));
              else
                arrAuxiliar[3] = '';
              //4.  sum debitos
              if (!feamultibook) {
                if (objResult[i].getValue(columns[2]) != null && objResult[i].getValue(columns[2]) != '- None -' && objResult[i].getValue(columns[2]) != 'NaN' && objResult[i].getValue(columns[2]) != 'undefined')
                  arrAuxiliar[4] = objResult[i].getValue(columns[2]);
                else
                  arrAuxiliar[4] = 0.0;
              } else {
                if (objResult[i].getValue(columns[13]) != null && objResult[i].getValue(columns[13]) != '- None -' && objResult[i].getValue(columns[13]) != 'NaN' && objResult[i].getValue(columns[13]) != 'undefined')
                  arrAuxiliar[4] = objResult[i].getValue(columns[13]);
                else
                  arrAuxiliar[4] = 0.0;
              }
              //5.  sum credito
              if (!feamultibook) {
                if (objResult[i].getValue(columns[3]) != null && objResult[i].getValue(columns[3]) != '- None -' && objResult[i].getValue(columns[3]) != 'NaN' && objResult[i].getValue(columns[3]) != 'undefined')
                  arrAuxiliar[5] = objResult[i].getValue(columns[3]);
                else
                  arrAuxiliar[5] = 0.0;
              } else {
                if (objResult[i].getValue(columns[14]) != null && objResult[i].getValue(columns[14]) != '- None -' && objResult[i].getValue(columns[14]) != 'NaN' && objResult[i].getValue(columns[14]) != 'undefined')
                  arrAuxiliar[5] = objResult[i].getValue(columns[14]);
                else
                  arrAuxiliar[5] = 0.0;
              }
              //6.  cuenta 4 digitos
              if (objResult[i].getValue(columns[4]) != null && objResult[i].getValue(columns[4]) != '- None -' && objResult[i].getValue(columns[4]) != 'NaN' && objResult[i].getValue(columns[4]) != 'undefined')
                arrAuxiliar[6] = objResult[i].getValue(columns[4]);
              else
                arrAuxiliar[6] = '';
              //7.  denominacion 4 digitos
              if (objResult[i].getValue(columns[5]) != null && objResult[i].getValue(columns[5]) != '- None -' && objResult[i].getValue(columns[5]) != 'NaN' && objResult[i].getValue(columns[5]) != 'undefined')
                arrAuxiliar[7] = ValidarAcentos(objResult[i].getValue(columns[5]));
              else
                arrAuxiliar[7] = '';
              //8.  cuenta 2 digitos
              if (objResult[i].getValue(columns[6]) != null && objResult[i].getValue(columns[6]) != '- None -' && objResult[i].getValue(columns[6]) != 'NaN' && objResult[i].getValue(columns[6]) != 'undefined')
                arrAuxiliar[8] = objResult[i].getValue(columns[6]);
              else
                arrAuxiliar[8] = '';
              //9.  denominacion 2 digitos
              if (objResult[i].getValue(columns[7]) != null && objResult[i].getValue(columns[7]) != '- None -' && objResult[i].getValue(columns[7]) != 'NaN' && objResult[i].getValue(columns[7]) != 'undefined')
                arrAuxiliar[9] = ValidarAcentos(objResult[i].getValue(columns[7]));
              else
                arrAuxiliar[9] = '';
              //10.  cuenta 1 digito
              if (objResult[i].getValue(columns[8]) != null && objResult[i].getValue(columns[8]) != '- None -' && objResult[i].getValue(columns[8]) != 'NaN' && objResult[i].getValue(columns[8]) != 'undefined')
                arrAuxiliar[10] = objResult[i].getValue(columns[8]);
              else
                arrAuxiliar[10] = '';
              //11.  denominacion 1 digito
              if (objResult[i].getValue(columns[9]) != null && objResult[i].getValue(columns[9]) != '- None -' && objResult[i].getValue(columns[9]) != 'NaN' && objResult[i].getValue(columns[9]) != 'undefined')
                arrAuxiliar[11] = ValidarAcentos(objResult[i].getValue(columns[9]));
              else
                arrAuxiliar[11] = '';
              //12. saldo
              if (!feamultibook) {
                if (objResult[i].getValue(columns[10]) != null && objResult[i].getValue(columns[10]) != '- None -' && objResult[i].getValue(columns[10]) != 'NaN' && objResult[i].getValue(columns[10]) != 'undefined')
                  arrAuxiliar[12] = objResult[i].getValue(columns[10]);
                else
                  arrAuxiliar[12] = 0.0;
              } else {
                arrAuxiliar[12] = arrAuxiliar[4] - arrAuxiliar[5];
              }

              //13. tipo cuenta
              if (objResult[i].getValue(columns[11]) != null && objResult[i].getValue(columns[11]) != '- None -' && objResult[i].getValue(columns[11]) != 'NaN' && objResult[i].getValue(columns[11]) != 'undefined')
                arrAuxiliar[13] = ValidarAcentos(objResult[i].getText(columns[11]));
              else
                arrAuxiliar[13] = '';
              //14. number
              if (feamultibook) {
                if (objResult[i].getValue(columns[15]) != null && objResult[i].getValue(columns[15]) != '- None -' && objResult[i].getValue(columns[15]) != 'NaN' && objResult[i].getValue(columns[15]) != 'undefined')
                  arrAuxiliar[14] = objResult[i].getValue(columns[15]);
                else
                  arrAuxiliar[14] = '';
              } else {
                if (objResult[i].getValue(columns[12]) != null && objResult[i].getValue(columns[12]) != '- None -' && objResult[i].getValue(columns[12]) != 'NaN' && objResult[i].getValue(columns[12]) != 'undefined')
                  arrAuxiliar[14] = objResult[i].getValue(columns[12]);
                else
                  arrAuxiliar[14] = '';
              }

              //15
              if (feamultibook) {
                var val_trainid = objResult[i].getValue(columns[16]) != null && objResult[i].getValue(columns[16]) != '- None -' && objResult[i].getValue(columns[16]) != 'NaN' && objResult[i].getValue(columns[16]) != 'undefined';
                var val_type = objResult[i].getValue(columns[17]) != null && objResult[i].getValue(columns[17]) != '- None -' && objResult[i].getValue(columns[17]) != 'NaN' && objResult[i].getValue(columns[17]) != 'undefined';
                if (val_type)
                  arrAuxiliar[15] = objResult[i].getText(columns[17]);
                else
                  arrAuxiliar[15] = '';

                if (val_trainid)
                  arrAuxiliar[15] += ' - ' + objResult[i].getValue(columns[16]);

              } else {
                var val_trainid = objResult[i].getValue(columns[13]) != null && objResult[i].getValue(columns[13]) != '- None -' && objResult[i].getValue(columns[13]) != 'NaN' && objResult[i].getValue(columns[13]) != 'undefined';

                var val_type = objResult[i].getValue(columns[14]) != null && objResult[i].getValue(columns[14]) != '- None -' && objResult[i].getValue(columns[14]) != 'NaN' && objResult[i].getValue(columns[14]) != 'undefined';

                if (val_type)
                  arrAuxiliar[15] = objResult[i].getText(columns[14]);
                else
                  arrAuxiliar[15] = '';
                if (val_trainid)
                  arrAuxiliar[15] += " - " + objResult[i].getValue(columns[13]);
              }
            } else {
              /*
              * 0.  cuenta 6 digitos
              * 1.  denominacion 6 digitos
              * 2.  sum debitos
              * 3.  sum credito
              * 4.  cuenta 4 digitos
              * 5.  denominacion 4 digitos
              * 6.  cuenta 2 digitos
              * 7.  denominacion 2 digitos
              * 8.  cuenta 1 digito
              * 9.  denominacion 1 digito
              * 10. period id
              * 11. period name
              * 12. saldo
              * 13. TIPO D CUENTA
              * 14.number
              */
              //0. cuenta 6 digitos
              if (objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '- None -' && objResult[i].getValue(columns[0]) != 'NaN' && objResult[i].getValue(columns[0]) != 'undefined')
                arrAuxiliar[0] = objResult[i].getValue(columns[0]);
              else
                arrAuxiliar[0] = '';
              //1. denominacion 6 digitos
              if (objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '- None -' && objResult[i].getValue(columns[1]) != 'NaN' && objResult[i].getValue(columns[1]) != 'undefined')
                arrAuxiliar[1] = ValidarAcentos(objResult[i].getValue(columns[1]));
              else
                arrAuxiliar[1] = '';
              //2.  sum debitos
              if (!feamultibook) {
                if (objResult[i].getValue(columns[2]) != null && objResult[i].getValue(columns[2]) != '- None -' && objResult[i].getValue(columns[2]) != 'NaN' && objResult[i].getValue(columns[2]) != 'undefined')
                  arrAuxiliar[2] = objResult[i].getValue(columns[2]);
                else
                  arrAuxiliar[2] = 0.0;
              } else {
                if (objResult[i].getValue(columns[13]) != null && objResult[i].getValue(columns[13]) != '- None -' && objResult[i].getValue(columns[13]) != 'NaN' && objResult[i].getValue(columns[13]) != 'undefined')
                  arrAuxiliar[2] = objResult[i].getValue(columns[13]);
                else
                  arrAuxiliar[2] = 0.0;
              }
              //3.  sum credito
              if (!feamultibook) {
                if (objResult[i].getValue(columns[3]) != null && objResult[i].getValue(columns[3]) != '- None -' && objResult[i].getValue(columns[3]) != 'NaN' && objResult[i].getValue(columns[3]) != 'undefined')
                  arrAuxiliar[3] = objResult[i].getValue(columns[3]);
                else
                  arrAuxiliar[3] = 0.0;
              } else {
                if (objResult[i].getValue(columns[14]) != null && objResult[i].getValue(columns[14]) != '- None -' && objResult[i].getValue(columns[14]) != 'NaN' && objResult[i].getValue(columns[14]) != 'undefined')
                  arrAuxiliar[3] = objResult[i].getValue(columns[14]);
                else
                  arrAuxiliar[3] = 0.0;
              }
              //4.  cuenta 4 digitos
              if (objResult[i].getValue(columns[4]) != null && objResult[i].getValue(columns[4]) != '- None -' && objResult[i].getValue(columns[4]) != 'NaN' && objResult[i].getValue(columns[4]) != 'undefined')
                arrAuxiliar[4] = objResult[i].getValue(columns[4]);
              else
                arrAuxiliar[4] = '';
              //5.  denominacion 4 digitos
              if (objResult[i].getValue(columns[5]) != null && objResult[i].getValue(columns[5]) != '- None -' && objResult[i].getValue(columns[5]) != 'NaN' && objResult[i].getValue(columns[5]) != 'undefined')
                arrAuxiliar[5] = ValidarAcentos(objResult[i].getValue(columns[5]));
              else
                arrAuxiliar[5] = '';
              //6.  cuenta 2 digitos
              if (objResult[i].getValue(columns[6]) != null && objResult[i].getValue(columns[6]) != '- None -' && objResult[i].getValue(columns[6]) != 'NaN' && objResult[i].getValue(columns[6]) != 'undefined')
                arrAuxiliar[6] = objResult[i].getValue(columns[6]);
              else
                arrAuxiliar[6] = '';
              //7.  denominacion 2 digitos
              if (objResult[i].getValue(columns[7]) != null && objResult[i].getValue(columns[7]) != '- None -' && objResult[i].getValue(columns[7]) != 'NaN' && objResult[i].getValue(columns[7]) != 'undefined')
                arrAuxiliar[7] = ValidarAcentos(objResult[i].getValue(columns[7]));
              else
                arrAuxiliar[7] = '';
              //8.  cuenta 1 digito
              if (objResult[i].getValue(columns[8]) != null && objResult[i].getValue(columns[8]) != '- None -' && objResult[i].getValue(columns[8]) != 'NaN' && objResult[i].getValue(columns[8]) != 'undefined')
                arrAuxiliar[8] = objResult[i].getValue(columns[8]);
              else
                arrAuxiliar[8] = '';
              //9.  denominacion 1 digito
              if (objResult[i].getValue(columns[9]) != null && objResult[i].getValue(columns[9]) != '- None -' && objResult[i].getValue(columns[9]) != 'NaN' && objResult[i].getValue(columns[9]) != 'undefined')
                arrAuxiliar[9] = ValidarAcentos(objResult[i].getValue(columns[9]));
              else
                arrAuxiliar[9] = '';
              //10. saldo
              if (!feamultibook) {
                if (objResult[i].getValue(columns[10]) != null && objResult[i].getValue(columns[10]) != '- None -' && objResult[i].getValue(columns[10]) != 'NaN' && objResult[i].getValue(columns[10]) != 'undefined')
                  arrAuxiliar[10] = objResult[i].getValue(columns[10]);
                else
                  arrAuxiliar[10] = 0.0;
              } else {
                arrAuxiliar[10] = arrAuxiliar[2] - arrAuxiliar[3];
              }

              //11. tipo cuenta
              if (objResult[i].getValue(columns[11]) != null && objResult[i].getValue(columns[11]) != '- None -' && objResult[i].getValue(columns[11]) != 'NaN' && objResult[i].getValue(columns[11]) != 'undefined')
                arrAuxiliar[11] = ValidarAcentos(objResult[i].getText(columns[11]));
              else
                arrAuxiliar[11] = '';
              //12. number
              if (feamultibook) {
                if (objResult[i].getValue(columns[15]) != null && objResult[i].getValue(columns[15]) != '- None -' && objResult[i].getValue(columns[15]) != 'NaN' && objResult[i].getValue(columns[15]) != 'undefined')
                  arrAuxiliar[12] = objResult[i].getValue(columns[15]);
                else
                  arrAuxiliar[12] = '';
              } else {
                if (objResult[i].getValue(columns[12]) != null && objResult[i].getValue(columns[12]) != '- None -' && objResult[i].getValue(columns[12]) != 'NaN' && objResult[i].getValue(columns[12]) != 'undefined')
                  arrAuxiliar[12] = objResult[i].getValue(columns[12]);
                else
                  arrAuxiliar[12] = '';
              }

              //13
              if (feamultibook) {
                var val_trainid = objResult[i].getValue(columns[16]) != null && objResult[i].getValue(columns[16]) != '- None -' && objResult[i].getValue(columns[16]) != 'NaN' && objResult[i].getValue(columns[16]) != 'undefined';
                var val_type = objResult[i].getValue(columns[17]) != null && objResult[i].getValue(columns[17]) != '- None -' && objResult[i].getValue(columns[17]) != 'NaN' && objResult[i].getValue(columns[17]) != 'undefined';
                if (val_type)
                  arrAuxiliar[13] = objResult[i].getText(columns[17]);
                else
                  arrAuxiliar[13] = '';

                if (val_trainid)
                  arrAuxiliar[13] += ' - ' + objResult[i].getValue(columns[16]);

              } else {
                var val_trainid = objResult[i].getValue(columns[13]) != null && objResult[i].getValue(columns[13]) != '- None -' && objResult[i].getValue(columns[13]) != 'NaN' && objResult[i].getValue(columns[13]) != 'undefined';

                var val_type = objResult[i].getValue(columns[14]) != null && objResult[i].getValue(columns[14]) != '- None -' && objResult[i].getValue(columns[14]) != 'NaN' && objResult[i].getValue(columns[14]) != 'undefined';

                if (val_type)
                  arrAuxiliar[13] = objResult[i].getText(columns[14]);
                else
                  arrAuxiliar[13] = '';
                if (val_trainid)
                  arrAuxiliar[13] += " - " + objResult[i].getValue(columns[13]);
              }
            }

            if (feamultibook) {
              //Cambios de cuenta multibook para saldos anteriores
              if (paramOchoDigitos == 'T') {
                var x = jsonIndexCuentas.get(arrAuxiliar[14]);
                if (x != undefined) {
                  arrAuxiliar[0] = ArrCuentas[x][10];
                  arrAuxiliar[1] = ArrCuentas[x][11];
                  arrAuxiliar[2] = ArrCuentas[x][8];
                  arrAuxiliar[3] = ArrCuentas[x][9];
                  arrAuxiliar[6] = ArrCuentas[x][6];
                  arrAuxiliar[7] = ArrCuentas[x][7];
                  arrAuxiliar[8] = ArrCuentas[x][4];
                  arrAuxiliar[9] = ArrCuentas[x][5];
                  arrAuxiliar[10] = ArrCuentas[x][2];
                  arrAuxiliar[11] = ArrCuentas[x][3];
                }
              } else {
                var x = jsonIndexCuentas.get(arrAuxiliar[12]);
                if (x != undefined) {
                  arrAuxiliar[0] = ArrCuentas[x][8];
                  arrAuxiliar[1] = ArrCuentas[x][9];
                  arrAuxiliar[4] = ArrCuentas[x][6];
                  arrAuxiliar[5] = ArrCuentas[x][7];
                  arrAuxiliar[6] = ArrCuentas[x][4];
                  arrAuxiliar[7] = ArrCuentas[x][5];
                  arrAuxiliar[8] = ArrCuentas[x][2];
                  arrAuxiliar[9] = ArrCuentas[x][3];
                }
              }
            }

            var tempPuc = arrAuxiliar[0].substring(0, 1);
            if (tempPuc == 4 || tempPuc == 5 || tempPuc == 6 || tempPuc == 7) {
              ArrMontosAux.push(arrAuxiliar);
            }
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

      return ArrMontosAux;
    }

    function puc8digDescription() {
      // Control de Memoria
      var intDMaxReg = 1000;
      var intDMinReg = 0;
      var DbolStop = false;
      //1. denominacion 8 digitos
      var descripcion8digitSearchObj = search.create({
        type: 'customrecord_lmry_co_puc',
        filters: [],
        columns: ['name', 'custrecord_lmry_co_puc']
      });
      var filter_search_puc8 = search.createFilter({
        name: 'formulatext',
        formula: "LENGTH({name})",
        operator: 'is',
        values: 8
      });
      descripcion8digitSearchObj.filters.push(filter_search_puc8);
      var varResult = descripcion8digitSearchObj.run();
      while (!DbolStop) {
        var objResult = varResult.getRange(intDMinReg, intDMaxReg);
        if (objResult != null) {
          var intLength = objResult.length;
          if (intLength == 0) {
            DbolStop = true;
          } else {
            for (var i = 0; i < intLength; i++) {
              // Cantidad de columnas de la busqueda
              columns = objResult[i].columns;
              var key = ValidarAcentos(objResult[i].getValue(columns[0]));
              var value = ValidarAcentos(objResult[i].getValue(columns[1]));
              puc8DigDescript[key] = value;
            }
          }
          intDMinReg = intDMaxReg;
          intDMaxReg += 1000;
        } else {
          DbolStop = true;
        }
      }
      //log.debug('puc8DigDescript', puc8DigDescript);
    }


    function CapturaInfoPeriodo(pParamPeriodo) {
      var yearDateYYArray = new Array();

      if (featAccountingSpecial || featAccountingSpecial == true) {
        yearDateYYArray = periodstartdateSpecial.split('/');

      } else {
        yearDateYYArray = periodstartdate.split('/');
      }
      yearDateYY = yearDateYYArray[2];
      yearDateMM = yearDateYYArray[1];

      for (var cCont = 0; cCont < ArrPeriodos.length; cCont++) {
        if (cCont != 0) {
          if (pParamPeriodo == ArrPeriodos[cCont][0]) {
            periodstartdate = ArrPeriodos[cCont][2];
            periodenddate = ArrPeriodos[cCont][3];
            //periodname = ArrPeriodos[cCont][1];
            antperiodenddate = ArrPeriodos[cCont - 1][3];

            return true;
          }
        }
      }
      //se resta para saber
      var idPeriEjem = parseInt(pParamPeriodo) - 1;

      for (var cCont = 0; cCont < ArrPeriodos.length; cCont++) {
        if ('' + idPeriEjem == ArrPeriodos[cCont][0]) {

          antperiodenddate = ArrPeriodos[cCont][3];
          var cToday = format.parse({
            value: antperiodenddate,
            type: format.Type.DATE
          });
          var cYear = cToday.getFullYear().toFixed(0);
          periodstartdate = '12/31/' + cYear;
          periodenddate = '12/31/' + cYear;
          //periodname = ArrPeriodos[cCont][1];
          flagAjuste = true;

          return true;

        }
      }
    }

    function ObtienePeriodoContable() {
      // Control de Memoria
      var intDMaxReg = 1000;
      var intDMinReg = 0;
      var arrAuxiliar = new Array();

      // Exedio las unidades
      var DbolStop = false;
      var _cont = 0;

      // Consulta de Cuentas
      var filters = new Array();

      filters[0] = search.createFilter({
        name: 'isinactive',
        operator: search.Operator.IS,
        values: 'F'
      });
      filters[1] = search.createFilter({
        name: 'isquarter',
        operator: search.Operator.IS,
        values: 'F'
      });
      filters[2] = search.createFilter({
        name: 'isyear',
        operator: search.Operator.IS,
        values: 'F'
      });

      if (paramAdjustment == 'F') {
        filters[3] = search.createFilter({
          name: 'isadjust',
          operator: search.Operator.IS,
          values: false
        });
      }

      var columns = new Array();

      columns[0] = search.createColumn('internalid');
      columns[1] = search.createColumn('periodname');
      columns[2] = search.createColumn({
        name: 'startdate',
        sort: search.Sort.ASC
      });
      columns[3] = search.createColumn('enddate');
      columns[4] = search.createColumn({
        name: 'formulatext',
        formula: "TO_CHAR({startdate},'yyyy')"
      });

      var savedsearch = search.create({
        type: 'accountingperiod',
        filters: filters,
        columns: columns
      })
      var searchresult = savedsearch.run();

      while (!DbolStop) {
        var objResult = searchresult.getRange(intDMinReg, intDMaxReg);
        if (objResult != null) {
          var intLength = objResult.length;

          for (var i = 0; i < intLength; i++) {
            columns = objResult[i].columns;

            arrAuxiliar = new Array();

            //0. Internal Id
            if (objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '- None -' && objResult[i].getValue(columns[0]) != 'NaN' && objResult[i].getValue(columns[0]) != 'undefined')
              arrAuxiliar[0] = objResult[i].getValue(columns[0]);
            else
              arrAuxiliar[0] = '';
            //1. name
            if (objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '- None -' && objResult[i].getValue(columns[1]) != 'NaN' && objResult[i].getValue(columns[1]) != 'undefined')
              arrAuxiliar[1] = objResult[i].getValue(columns[1]);
            else
              arrAuxiliar[1] = '';
            //2. fecha inicial
            if (objResult[i].getValue(columns[2]) != null && objResult[i].getValue(columns[2]) != '- None -' && objResult[i].getValue(columns[2]) != 'NaN' && objResult[i].getValue(columns[2]) != 'undefined')
              arrAuxiliar[2] = objResult[i].getValue(columns[2]);
            else
              arrAuxiliar[2] = '';
            //3. fecha final
            if (objResult[i].getValue(columns[3]) != null && objResult[i].getValue(columns[3]) != '- None -' && objResult[i].getValue(columns[3]) != 'NaN' && objResult[i].getValue(columns[3]) != 'undefined')
              arrAuxiliar[3] = objResult[i].getValue(columns[3]);
            else
              arrAuxiliar[3] = '';
            //4. is Ajuste

            ArrPeriodos[_cont] = arrAuxiliar;
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
    }

    function obtenerPeriodosEspeciales(paramperiod) {
      var startDate;
      var periodName;
      var endDate;

      if (featAccountingSpecial || featAccountingSpecial == 'T') {

        var searchSpecialPeriod = search.create({
          type: "customrecord_lmry_special_accountperiod",
          filters: [
            ["isinactive", "is", "F"], 'AND',
            ["custrecord_lmry_accounting_period", "is", paramperiod]
          ],
          columns: [
            search.createColumn({
              name: "custrecord_lmry_date_ini",
              label: "1. Latam - Date Start",
            }),
            search.createColumn({
              name: "custrecord_lmry_date_fin",
              label: "2. Latam - Date Fin",
            }),
            search.createColumn({
              name: "name",
              label: "3. Latam - Period Name",
            })
          ]
        });

        if (featureMultipCalendars || featureMultipCalendars == 'T') {
          var fiscalCalendarFilter = search.createFilter({
            name: 'custrecord_lmry_calendar',
            operator: search.Operator.IS,
            values: calendarSubsi
          });
          searchSpecialPeriod.filters.push(fiscalCalendarFilter);
        }

        var pagedData = searchSpecialPeriod.runPaged({
          pageSize: 1000
        });

        pagedData.pageRanges.forEach(function (pageRange) {
          page = pagedData.fetch({
            index: pageRange.index
          });

          page.data.forEach(function (result) {
            columns = result.columns;
            startDate = result.getValue(columns[0]);
            endDate = result.getValue(columns[1]);
            periodName = result.getValue(columns[2]);

          })
        });
      } else {
        if (paramperiod != null && paramperiod != '') {
          var columnFrom = search.lookupFields({
            type: 'accountingperiod',
            id: paramperiod,
            columns: ['enddate', 'periodname', 'startdate']
          });
          startDate = columnFrom.startdate;
          endDate = columnFrom.enddate;
          periodName = columnFrom.periodname;
        }
      }

      return startDate + ',' + endDate + ',' + periodName;
    }
    /**********************************************************
     *Graba el archivo en el Gabinete de Archivos
     *********************************************************/
    function savefile(pNombreFile, pTipoArchivo, strName, count) {
      // Ruta de la carpeta contenedora
      var FolderId = objContext.getParameter({
        name: 'custscript_lmry_file_cabinet_rg_co'
      });

      // Almacena en la carpeta de Archivos Generados
      if (FolderId != '' && FolderId != null) {
        // Genera el nombre del archivo
        var NameFile = pNombreFile;

        // Crea el archivo
        var File = fileModulo.create({
          name: NameFile,
          fileType: pTipoArchivo,
          contents: strName,
          folder: FolderId
        });

        // Termina de grabar el archivo
        var idfile = File.save();

        // Trae URL de archivo generado
        var idfile2 = fileModulo.load({
          id: idfile
        });

        // Obtenemo de las prefencias generales el URL de Netsuite (Produccion o Sandbox)
        var getURL = objContext.getParameter({
          name: 'custscript_lmry_netsuite_location'
        });
        var urlfile = '';
        if (getURL != '' && getURL != '') {
          urlfile += 'https://' + getURL;
        }
        urlfile += idfile2.url;

        //Genera registro personalizado como log
        if (idfile) {
          var usuarioTemp = runtime.getCurrentUser();
          var usuario = usuarioTemp.name;
          var tmdate = new Date();
          var myDate = format.parse({
            value: tmdate,
            type: format.Type.TEXT
          });
          var myTime = format.parse({
            value: tmdate,
            type: format.Type.TIMEOFDAY
          });
          var current_date = myDate + ' ' + myTime;

          if (count > 1) {
            var record = recordModulo.create({
              type: RecordName
            });
            record.setValue({
              fieldId: 'custrecord_lmry_co_rg_transaction',
              value: 'CO - Libro Mayor y Balance (Detallado)'
            });
            //log.debug('periodname savefile',periodname);
            record.setValue({
              fieldId: RecordTable[1],
              value: periodname,
            });

            record.setValue({
              fieldId: RecordTable[2],
              value: companyname,
            });

            record.setValue({
              fieldId: RecordTable[4],
              value: usuario,
            });

            if (feamultibook || feamultibook == 'T') {
              record.setValue({
                fieldId: RecordTable[5],
                value: multibook_name.name
              }); // multibook
            }
          } else {
            var record = recordModulo.load({
              type: RecordName,
              id: paramidlog
            });
          }
          record.setValue({
            fieldId: RecordTable[0],
            value: NameFile,
          });

          record.setValue({
            fieldId: RecordTable[3],
            value: urlfile,
          });

          record.save({
            enableSourcing: true,
          })
          // Envia mail de conformidad al usuario
          libraryRPT.sendConfirmUserEmail('Latam CO - Libro Mayor', 3, NameFile, language);

        }
      } else {
        // Debug
        log.debug('[savefile] Creación de EXCEL', 'No existe el folder');
      }
    }


    function noData(mensaje) {

      var usuarioTemp = runtime.getCurrentUser();
      var usuario = usuarioTemp.name;

      var record = recordModulo.load({
        type: RecordName,
        id: paramidlog
      });

      //Nombre de Archivo
      record.setValue({
        fieldId: RecordTable[0],
        value: mensaje
      });
      //Creado Por
      record.setValue({
        fieldId: RecordTable[4],
        value: usuario
      });

      record.setValue({
        fieldId: RecordTable[1],
        value: periodname
      });

      var recordId = record.save();
    }



    function GeneraLibroMayorBalance(ArrMontosFinal, count) {

      if (featAccountingSpecial || featAccountingSpecial == true) {
        periodstartdate = periodstartdateSpecial;
        periodenddate = periodenddateSpecial;
      }

      if (ArrMontosFinal.length != null && ArrMontosFinal.length != 0 && !flagEmpty) {

        periodstartdate = ordenarFormatoFechas(periodstartdate);
        periodenddate = ordenarFormatoFechas(periodenddate);

        var xlsArchivo = '';
        xlsArchivo = '<?xml version="1.0" encoding="UTF-8" ?><?mso-application progid="Excel.Sheet"?>';
        xlsArchivo += '<Workbook xmlns="urn:schemas-microsoft-com:office:spreadsheet" ';
        xlsArchivo += 'xmlns:o="urn:schemas-microsoft-com:office:office" ';
        xlsArchivo += 'xmlns:x="urn:schemas-microsoft-com:office:excel" ';
        xlsArchivo += 'xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet" ';
        xlsArchivo += 'xmlns:html="http://www.w3.org/TR/REC-html40">';
        xlsArchivo += '<Styles>';
        xlsArchivo += '<Style ss:ID="s21"><Font ss:Bold="1" ss:Size="12" /><Alignment ss:Horizontal="Center" ss:Vertical="Bottom"/></Style>';
        xlsArchivo += '<Style ss:ID="s22"><Font ss:Bold="1"/><Alignment ss:Vertical="Bottom"/></Style>';
        xlsArchivo += '<Style ss:ID="s23"><Font ss:Bold="1"/><Alignment ss:Vertical="Bottom"/><NumberFormat ss:Format="_(* #,##0.00_);_(* \(#,##0.00\);_(* &quot;-&quot;??_);_(@_)"/></Style>';
        xlsArchivo += '<Style ss:ID="s24"><NumberFormat ss:Format="_(* #,##0.00_);_(* \(#,##0.00\);_(* &quot;-&quot;??_);_(@_)"/></Style>';
        xlsArchivo += '</Styles><Worksheet ss:Name="Sheet1">';


        xlsArchivo += '<Table>';
        xlsArchivo += '<Column ss:AutoFitWidth="0" ss:Width="100"/>';
        xlsArchivo += '<Column ss:AutoFitWidth="0" ss:Width="150"/>';
        xlsArchivo += '<Column ss:AutoFitWidth="0" ss:Width="100"/>';
        xlsArchivo += '<Column ss:AutoFitWidth="0" ss:Width="100"/>';
        xlsArchivo += '<Column ss:AutoFitWidth="0" ss:Width="100"/>';
        xlsArchivo += '<Column ss:AutoFitWidth="0" ss:Width="100"/>';
        xlsArchivo += '<Column ss:AutoFitWidth="0" ss:Width="100"/>';
        xlsArchivo += '<Column ss:AutoFitWidth="0" ss:Width="100"/>';
        xlsArchivo += '<Column ss:AutoFitWidth="0" ss:Width="100"/>';//gadp
        //Cabecera
        var xlsCabecera = '';
        xlsCabecera = '<Row>';
        xlsCabecera += '<Cell></Cell>';
        xlsCabecera += '<Cell></Cell>';
        xlsCabecera += '<Cell></Cell>';
        xlsCabecera += '<Cell ss:StyleID="s21"><Data ss:Type="String">' + GLOBAL_LABELS["Alert1"][language] + '</Data></Cell>';
        xlsCabecera += '</Row>';
        xlsCabecera += '<Row></Row>';
        xlsCabecera += '<Row>';
        xlsCabecera += '<Cell></Cell>';
        xlsCabecera += '<Cell></Cell>';
        xlsCabecera += '<Cell  ss:StyleID="s22"><Data ss:Type="String">' + GLOBAL_LABELS["Alert2"][language] + companyname + '</Data></Cell>';
        xlsCabecera += '</Row>';
        xlsCabecera += '<Row>';
        xlsCabecera += '<Cell></Cell>';
        xlsCabecera += '<Cell></Cell>';
        xlsCabecera += '<Cell  ss:StyleID="s22"><Data ss:Type="String">NIT: ' + companyruc + '</Data></Cell>';
        xlsCabecera += '</Row>';
        xlsCabecera += '<Row>';
        xlsCabecera += '<Cell></Cell>';
        xlsCabecera += '<Cell></Cell>';
        xlsCabecera += '<Cell  ss:StyleID="s22"><Data ss:Type="String">' + GLOBAL_LABELS["Alert3"][language] + periodstartdate + ' al ' + periodenddate + '</Data></Cell>';
        xlsCabecera += '</Row>';
        if ((feamultibook || feamultibook == 'T') && (paramMultibook != '' && paramMultibook != null)) {
          xlsCabecera += '<Row>';
          xlsCabecera += '<Cell></Cell>';
          xlsCabecera += '<Cell></Cell>';
          xlsCabecera += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + GLOBAL_LABELS["Alert4"][language] + multibook_name.name + '</Data></Cell>';
          xlsCabecera += '</Row>';
        }
        //PDF Normalized
        xlsCabecera += '<Row>';
        xlsCabecera += '<Cell></Cell>';
        xlsCabecera += '<Cell></Cell>';
        xlsCabecera += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + GLOBAL_LABELS["origin"][language] + "Netsuite" + '</Data></Cell>';
        xlsCabecera += '</Row>';
        xlsCabecera += '<Row>';
        xlsCabecera += '<Cell></Cell>';
        xlsCabecera += '<Cell></Cell>';
        xlsCabecera += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + GLOBAL_LABELS["date"][language] + todays + '</Data></Cell>';
        xlsCabecera += '</Row>';
        xlsCabecera += '<Row>';
        xlsCabecera += '<Cell></Cell>';
        xlsCabecera += '<Cell></Cell>';
        xlsCabecera += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + GLOBAL_LABELS["time"][language] + currentTime + '</Data></Cell>';
        xlsCabecera += '</Row>';
        //PDF Normalized End

        xlsCabecera += '<Row></Row>';
        xlsCabecera += '<Row></Row>';
        xlsCabecera += '<Row>' +
          '<Cell></Cell>' +
          '<Cell></Cell>' +
          '<Cell></Cell>' +
          '<Cell ss:MergeAcross="1" ss:StyleID="s21"><Data ss:Type="String">' + GLOBAL_LABELS["Alert7"][language] + '</Data></Cell>' +
          //'<Cell></Cell>' +
          '<Cell ss:MergeAcross="1" ss:StyleID="s21"><Data ss:Type="String">' + GLOBAL_LABELS["Alert8"][language] + '</Data></Cell>' +
          //'<Cell></Cell>' +
          '<Cell ss:MergeAcross="1" ss:StyleID="s21"><Data ss:Type="String">' + GLOBAL_LABELS["Alert9"][language] + '</Data></Cell>' +
          '</Row>';

        xlsCabecera += '<Row>' +
          '<Cell ss:StyleID="s21"><Data ss:Type="String">' + GLOBAL_LABELS["Alert5"][language] + '</Data></Cell>' +
          '<Cell ss:StyleID="s21"><Data ss:Type="String">' + GLOBAL_LABELS["Alert6"][language] + '</Data></Cell>' +
          '<Cell ss:StyleID="s21"><Data ss:Type="String">' + GLOBAL_LABELS["Alert14"][language] + '</Data></Cell>' +
          '<Cell ss:StyleID="s21"><Data ss:Type="String">' + GLOBAL_LABELS["Alert10"][language] + '</Data></Cell>' +
          '<Cell ss:StyleID="s21"><Data ss:Type="String">' + GLOBAL_LABELS["Alert11"][language] + '</Data></Cell>' +
          '<Cell ss:StyleID="s21"><Data ss:Type="String">' + GLOBAL_LABELS["Alert10"][language] + '</Data></Cell>' +
          '<Cell ss:StyleID="s21"><Data ss:Type="String">' + GLOBAL_LABELS["Alert11"][language] + '</Data></Cell>' +
          '<Cell ss:StyleID="s21"><Data ss:Type="String">' + GLOBAL_LABELS["Alert10"][language] + '</Data></Cell>' +
          '<Cell ss:StyleID="s21"><Data ss:Type="String">' + GLOBAL_LABELS["Alert11"][language] + '</Data></Cell>' +
          '</Row>';

        xlsString = xlsArchivo + xlsCabecera;

        var _TotalSIDebe = 0.0;
        var _TotalSIHaber = 0.0;
        var _TotalMovDebe = 0.0;
        var _TotalMovHaber = 0.0;
        var _TotalSFDebe = 0.0;
        var _TotalSFHaber = 0.0;

        // Formato de la celda
        _StyleTxt = ' ss:StyleID="s22" ';
        _StyleNum = ' ss:StyleID="s23" ';

        var sal_anterior = 0;
        var mov = 0;
        var nuevo_saldo = 0;
        var contador = 0;

        for (var i = 0; i < ArrMontosFinal.length; i++) {

          if (Math.abs(ArrMontosFinal[i][2]) == 0 && Math.abs(ArrMontosFinal[i][3]) == 0 &&
            Math.abs(ArrMontosFinal[i][4]) == 0 && Math.abs(ArrMontosFinal[i][5]) == 0 &&
            Math.abs(ArrMontosFinal[i][6]) == 0 && Math.abs(ArrMontosFinal[i][7]) == 0) {
            contador++;
          } else {
            xlsString += '<Row>';

            //////////////////////////////////
            //numero de cuenta
            if (ArrMontosFinal[i][0] != '' || ArrMontosFinal[i][0] != null) {

              xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + ArrMontosFinal[i][0] + '</Data></Cell>';
            } else {
              xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String"></Data></Cell>';
            }

            //Denominacion

            if (ArrMontosFinal[i][1].length > 0) {
              var s = ValidarAcentos(ArrMontosFinal[i][1]);
              xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + s + '</Data></Cell>';
            } else {
              xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String"></Data></Cell>';
            }

            //Document

            var document = '';

            if (paramOchoDigitos == 'T') {
              document = ValidarAcentos(ArrMontosFinal[i][19]);
            } else {
              document = ValidarAcentos(ArrMontosFinal[i][17]);
            }

            xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + document + '</Data></Cell>';
            // }
            /////////////////////////////////
            if (ArrMontosFinal[i][0].length == 1 || ArrMontosFinal[i][0].length == 2 || ArrMontosFinal[i][0].length == 4 || ArrMontosFinal[i][0].length == 6 || ArrMontosFinal[i][0].length == 8) {
              var saldo_antes = redondear(Math.abs(ArrMontosFinal[i][2]) - Math.abs(ArrMontosFinal[i][3]));
              var saldo_actual = redondear(Math.abs(ArrMontosFinal[i][2]) + Math.abs(ArrMontosFinal[i][4]) - Math.abs(ArrMontosFinal[i][3]) - Math.abs(ArrMontosFinal[i][5]));

              if (saldo_antes < 0) {
                //Haber Antes
                xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + 0.0 + '</Data></Cell>';
                //Debe Antes
                xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(saldo_antes) + '</Data></Cell>';
              } else {
                if (saldo_antes > 0) {
                  //Haber Antes
                  xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(saldo_antes) + '</Data></Cell>';
                  //Debe Antes
                  xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + 0.0 + '</Data></Cell>';
                } else {
                  xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + 0.0 + '</Data></Cell>';
                  xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + 0.0 + '</Data></Cell>';
                }
              }

              //Haber Movimientos
              xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(ArrMontosFinal[i][4]) + '</Data></Cell>';
              //Debe Movimientos
              xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(ArrMontosFinal[i][5]) + '</Data></Cell>';

              if (saldo_actual < 0) {
                //Haber Saldo
                xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + 0.0 + '</Data></Cell>';

                //Debe Saldo
                xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(saldo_actual) + '</Data></Cell>';
              } else if (saldo_actual > 0) {
                //Haber Saldo
                xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(saldo_actual) + '</Data></Cell>';

                //Debe Saldo
                xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + 0.0 + '</Data></Cell>';

              }

              if (ArrMontosFinal[i][0].length == 1) {
                sal_anterior = Math.abs(ArrMontosFinal[i][2]) - Math.abs(ArrMontosFinal[i][3]);
                mov = Math.abs(ArrMontosFinal[i][4]) - Math.abs(ArrMontosFinal[i][5]);
                nuevo_saldo = Math.abs(ArrMontosFinal[i][6]) - Math.abs(ArrMontosFinal[i][7]);

                if (sal_anterior > 0) {
                  _TotalSIDebe += sal_anterior;
                } else {
                  _TotalSIHaber += sal_anterior;
                }

                _TotalMovDebe += Math.abs(ArrMontosFinal[i][4]);

                _TotalMovHaber += Math.abs(ArrMontosFinal[i][5]);

                if (nuevo_saldo > 0) {
                  _TotalSFDebe += nuevo_saldo;
                } else {
                  _TotalSFHaber += nuevo_saldo;
                }
              }
            } else {
              //Haber Antes
              xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(ArrMontosFinal[i][2]) + '</Data></Cell>';
              //Debe Antes
              xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(ArrMontosFinal[i][3]) + '</Data></Cell>';
              //Haber Movimientos
              xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(ArrMontosFinal[i][4]) + '</Data></Cell>';
              //Debe Movimientos
              xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(ArrMontosFinal[i][5]) + '</Data></Cell>';
              //Haber Saldo
              xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(ArrMontosFinal[i][6]) + '</Data></Cell>';
              //Debe Saldo
              xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(ArrMontosFinal[i][7]) + '</Data></Cell>';
            }
            xlsString += '</Row>';
          }

        }

        // CAMBIO 2016/04/14 - TOTALES
        xlsString += '<Row>';
        xlsString += '<Cell></Cell>';
        xlsString += '<Cell ' + _StyleTxt + '><Data ss:Type="String">' + GLOBAL_LABELS["Alert13"][language] + '</Data></Cell>';
        xlsString += '<Cell></Cell>';
        xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(_TotalSIDebe) + '</Data></Cell>';
        xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(_TotalSIHaber) + '</Data></Cell>';
        xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(_TotalMovDebe) + '</Data></Cell>';
        xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(_TotalMovHaber) + '</Data></Cell>';
        xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(_TotalSFDebe) + '</Data></Cell>';
        xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(_TotalSFHaber) + '</Data></Cell>';
        xlsString += '</Row>';

        xlsString += '</Table></Worksheet></Workbook>';

        DateMM = yearDateMM;
        DateYY = yearDateYY;

        // Se arma el archivo EXCEL
        var strName = encode.convert({
          string: xlsString,
          inputEncoding: encode.Encoding.UTF_8,
          outputEncoding: encode.Encoding.BASE_64
        })

        if (paramMultibook != '' && paramMultibook != null) {
          var NameFile = "COLibroMayorBalance_" + companyname + "_" + monthStartD + "_" + yearStartD + "_" + paramMultibook + "_" + count + ".xls";
        } else {
          var NameFile = "COLibroMayorBalance_" + companyname + "_" + monthStartD + "_" + yearStartD + "_" + count + ".xls";
        }

        if (contador == ArrMontosFinal.length) {
          var usuarioTemp = runtime.getCurrentUser();
          var usuario = usuarioTemp.name;
          if (paramidlog != null && paramidlog != '') {

            if (count > 1) {
              var record = recordModulo.create({
                type: RecordName
              });
              record.setValue({
                fieldId: 'custrecord_lmry_co_rg_transaction',
                value: 'CO - Libro Mayor y Balance (Detallado)'
              });
            } else {
              var record = recordModulo.load({
                type: RecordName,
                id: paramidlog
              });
            }

            record.setValue({
              fieldId: RecordTable[0],
              value: GLOBAL_LABELS["Alert12"][language],
            });

            record.setValue({
              fieldId: RecordTable[1],
              value: periodname,
            });

            record.setValue({
              fieldId: RecordTable[2],
              value: companyname,
            });

            record.setValue({
              fieldId: RecordTable[4],
              value: usuario,
            });

            if (feamultibook || feamultibook == 'T') {
              record.setValue({
                fieldId: RecordTable[5],
                value: multibook_name.name
              }); // multibook
            }

            record.save({
              enableSourcing: true,
            })

          }

        } else {

          flagPDF = true;
          savefile(NameFile, 'EXCEL', strName, count);
        }

      } else {
        var usuarioTemp = runtime.getCurrentUser();
        var usuario = usuarioTemp.name;
        if (paramidlog != null && paramidlog != '') {



          if (count > 1) {
            var record = recordModulo.create({
              type: RecordName
            });
            record.setValue({
              fieldId: 'custrecord_lmry_co_rg_transaction',
              value: 'CO - Libro Mayor y Balance (Detallado)'
            });
            record.setValue({
              fieldId: RecordTable[0],
              value: GLOBAL_LABELS["Alert12"][language],
            });

            record.setValue({
              fieldId: RecordTable[1],
              value: periodname,
            });

            record.setValue({
              fieldId: RecordTable[2],
              value: companyname,
            });

            record.setValue({
              fieldId: RecordTable[4],
              value: usuario,
            });

            if (feamultibook || feamultibook == 'T') {
              record.setValue({
                fieldId: RecordTable[5],
                value: multibook_name.name
              }); // multibook
            }
          } else {
            var record = recordModulo.load({
              type: RecordName,
              id: paramidlog
            });
          }
          record.setValue({
            fieldId: RecordTable[0],
            value: GLOBAL_LABELS["Alert12"][language],
          });

          record.save()
        }

      }
    }

    function redondear(number) {
      return Math.round(Number(number) * 100) / 100;
    }

    function ObtainNameSubsidiaria(subsidiari) {
      try {
        if (subsidiari != '' && subsidiari != null) {
          var Name = search.lookupFields({
            type: 'subsidiary',
            id: subsidiari,
            columns: 'legalname'
          })
          return Name.legalname;
        }
      } catch (err) {
        libraryRPT.sendErrorEmail(err, LMRY_script, language);
      }
      return '';
    }

    //-------------------------------------------------------------------------------------------------------
    //Obtiene el n?mero de identificaci?n fiscal de la subsidiaria
    //-------------------------------------------------------------------------------------------------------
    function ObtainFederalIdSubsidiaria(subsidiari) {
      try {
        if (subsidiari != '' && subsidiari != null) {
          var FederalIdNumber = search.lookupFields({
            type: search.Type.SUBSIDIARY,
            id: subsidiari,
            columns: ['taxidnum']
          });
          return FederalIdNumber.taxidnum;
        }
      } catch (err) {
        libraryRPT.sendErrorEmail(err, LMRY_script, language);
      }
      return '';
    }

    function ObtieneAccountingContext() {
      // Control de Memoria
      var intDMaxReg = 1000;
      var intDMinReg = 0;
      var arrAuxiliar = new Array();
      var contador_auxiliar = 0;
      var DbolStop = false;

      var savedsearch = search.load({
        id: 'customsearch_lmry_account_context',
      })

      // Valida si es OneWorld
      if (featuresubs == true) {
        savedsearch.filters.push(search.createFilter({
          name: 'subsidiary',
          operator: search.Operator.IS,
          values: [paramsubsidi]
        }));
      }

      if (paramOchoDigitos == 'T') {
        // VALIDAR QUE EL CAMPO TENGA 8 DIGITOS
        var filter_search_puc8 = search.createFilter({
          name: 'formulatext',
          formula: "LENGTH({custrecord_lmry_co_puc_id})",
          operator: 'is',
          values: 8
        });
        savedsearch.filters.push(filter_search_puc8);
        // PUC 8 DIG ID
        var col_search_puc8_id = search.createColumn({
          name: 'formulatext',
          summary: 'group',
          formula: '{custrecord_lmry_co_puc_id}'
        });
        savedsearch.columns.push(col_search_puc8_id);
        //PUC 8 DIG DESCRIPCION
        var col_search_puc8_den = search.createColumn({
          name: 'formulatext',
          summary: 'group',
          formula: '{custrecord_lmry_co_puc_id.custrecord_lmry_co_puc}'
        });
        savedsearch.columns.push(col_search_puc8_den);
      }

      var col_search_puc6_id = search.createColumn({
        name: 'formulatext',
        summary: 'group',
        formula: '{custrecord_lmry_co_puc_d6_id}'
      });
      savedsearch.columns.push(col_search_puc6_id);

      var col_search_puc6_den = search.createColumn({
        name: 'formulatext',
        summary: 'group',
        formula: '{custrecord_lmry_co_puc_d6_description}'
      });
      savedsearch.columns.push(col_search_puc6_den);

      var col_search_puc4_id = search.createColumn({
        name: 'formulatext',
        summary: 'group',
        formula: '{custrecord_lmry_co_puc_d4_id}'
      });
      savedsearch.columns.push(col_search_puc4_id);

      var col_search_puc4_den = search.createColumn({
        name: 'formulatext',
        summary: 'group',
        formula: '{custrecord_lmry_co_puc_d4_description}'
      });
      savedsearch.columns.push(col_search_puc4_den);

      var col_search_puc2_id = search.createColumn({
        name: 'formulatext',
        summary: 'group',
        formula: '{custrecord_lmry_co_puc_d2_id}'
      });
      savedsearch.columns.push(col_search_puc2_id);

      var col_search_puc2_den = search.createColumn({
        name: 'formulatext',
        summary: 'group',
        formula: '{custrecord_lmry_co_puc_d2_description}'
      });
      savedsearch.columns.push(col_search_puc2_den);

      var col_search_puc1_id = search.createColumn({
        name: 'formulatext',
        summary: 'group',
        formula: '{custrecord_lmry_co_puc_d1_id}'
      });
      savedsearch.columns.push(col_search_puc1_id);

      var col_search_puc1_den = search.createColumn({
        name: 'formulatext',
        summary: 'group',
        formula: '{custrecord_lmry_co_puc_d1_description}'
      });
      savedsearch.columns.push(col_search_puc1_den);

      var searchresult = savedsearch.run();


      while (!DbolStop) {
        var objResult = searchresult.getRange(intDMinReg, intDMaxReg);

        if (objResult != null) {
          var intLength = objResult.length;


          if (intLength == 0) {
            DbolStop = true;
          } else {
            for (var i = 0; i < intLength; i++) {
              // Cantidad de columnas de la busqueda
              columns = objResult[i].columns;

              arrAuxiliar = new Array();
              for (var col = 0; col < columns.length; col++) {
                if (col == 3) {
                  arrAuxiliar[col] = objResult[i].getText(columns[col]);
                } else {
                  arrAuxiliar[col] = ValidarAcentos(objResult[i].getValue(columns[col]));
                }
              }

              if (arrAuxiliar[3] == multibook_name['name']) {
                arrAccountingContext[contador_auxiliar] = arrAuxiliar;
                contador_auxiliar++;
              }
            }
          }

          intDMinReg = intDMaxReg;
          intDMaxReg += 1000;

        } else {
          DbolStop = true;
        }
      }
      // log.debug('arrAccountingContext',arrAccountingContext);
      return arrAccountingContext;
    }

    function obtenerCuenta(numero_cuenta) {
      var digitos = true;
      for (var i = 0; i < arrAccountingContext.length; i++) {
        if (numero_cuenta == arrAccountingContext[i][0]) {
          var number_cta_aux = arrAccountingContext[i][1];
          for (var j = 0; j < arrAccountingContext.length; j++) {

            if (number_cta_aux == arrAccountingContext[j][0]) {
              var ArrPuc = new Array();
              if (digitos == true) {
                ArrPuc[0] = arrAccountingContext[j][2];
                ArrPuc[1] = arrAccountingContext[j][3];
                ArrPuc[2] = arrAccountingContext[j][6];
                ArrPuc[3] = arrAccountingContext[j][7];
                ArrPuc[4] = arrAccountingContext[j][8];
                ArrPuc[5] = arrAccountingContext[j][9];
                ArrPuc[6] = arrAccountingContext[j][10];
                ArrPuc[7] = arrAccountingContext[j][11];
                ArrPuc[8] = arrAccountingContext[j][12];
                ArrPuc[9] = arrAccountingContext[j][13];
              } else {
                ArrPuc[0] = arrAccountingContext[j][4];
                ArrPuc[1] = arrAccountingContext[j][5];
                ArrPuc[2] = arrAccountingContext[j][6];
                ArrPuc[3] = arrAccountingContext[j][7];
                ArrPuc[4] = arrAccountingContext[j][8];
                ArrPuc[5] = arrAccountingContext[j][9];
                ArrPuc[6] = arrAccountingContext[j][10];
                ArrPuc[7] = arrAccountingContext[j][11];
              }
              return ArrPuc;
            }
          }
        }
      }
      return numero_cuenta;
    }

    function ValidarAcentos(s) {
      var AccChars = "ŠŽšžŸÀÁÂÃÄÅÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖÙÚÛÜÝàáâãäåçèéêëìíîïðñòóôõöùúûüýÿ&°–—ªº·";
      var RegChars = "SZszYAAAAAACEEEEIIIIDNOOOOOUUUUYaaaaaaceeeeiiiidnooooouuuuyyyo--ao.";

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

    function cambioDeCuentas() {
      var digitos = true;
      //log.debug('LLEGO HASTA ACA ArrMontosFinal', ArrMontosFinal);

      for (var i = 0; i < ArrMontosFinal.length; i++) {

        if (digitos == true) {
          if (ArrMontosFinal[i][17] == 'Bank' || ArrMontosFinal[i][17] == 'Accounts Payable' || ArrMontosFinal[i][17] == 'Accounts Receivable' ||
            ArrMontosFinal[i][17] == 'Banco' || ArrMontosFinal[i][17] == 'Cuentas a pagar' || ArrMontosFinal[i][17] == 'Cuentas a cobrar') {
            var cuenta_act = obtenerCuenta(ArrMontosFinal[i][18]);

            if (cuenta_act != ArrMontosFinal[i][18]) {
              ArrMontosFinal[i][0] = cuenta_act[0];
              ArrMontosFinal[i][1] = cuenta_act[1];
              ArrMontosFinal[i][8] = cuenta_act[2];
              ArrMontosFinal[i][9] = cuenta_act[3];
              ArrMontosFinal[i][10] = cuenta_act[4];
              ArrMontosFinal[i][11] = cuenta_act[5];
              ArrMontosFinal[i][12] = cuenta_act[6];
              ArrMontosFinal[i][13] = cuenta_act[7];
              ArrMontosFinal[i][14] = cuenta_act[8];
              ArrMontosFinal[i][15] = cuenta_act[9];
            }
          }
        } else {
          if (ArrMontosFinal[i][15] == 'Bank' || ArrMontosFinal[i][15] == 'Accounts Payable' || ArrMontosFinal[i][15] == 'Accounts Receivable' ||
            ArrMontosFinal[i][15] == 'Banco' || ArrMontosFinal[i][15] == 'Cuentas a pagar' || ArrMontosFinal[i][15] == 'Cuentas a cobrar') {
            var cuenta_act = obtenerCuenta(ArrMontosFinal[i][16]);

            if (cuenta_act != ArrMontosFinal[i][16]) {
              ArrMontosFinal[i][0] = cuenta_act[0];
              ArrMontosFinal[i][1] = cuenta_act[1];
              ArrMontosFinal[i][8] = cuenta_act[2];
              ArrMontosFinal[i][9] = cuenta_act[3];
              ArrMontosFinal[i][10] = cuenta_act[4];
              ArrMontosFinal[i][11] = cuenta_act[5];
              ArrMontosFinal[i][12] = cuenta_act[6];
              ArrMontosFinal[i][13] = cuenta_act[7];
            }
          }
        }
      }
      return ArrMontosFinal;
    }

    function ordenarFormatoFechas(periodoDate) {
      var tempdateReporte = format.parse({
        value: periodoDate,
        type: format.Type.DATE
      });

      var dayReport = null;
      var monthReport = null;
      var yearReport = null;
      var stringDate = (tempdateReporte + "").length;
      if (stringDate > 10) {
        dayReport = tempdateReporte.getDate();
        monthReport = tempdateReporte.getMonth() + 1;
        yearReport = tempdateReporte.getFullYear();
      } else {
        tempdateReporte = tempdateReporte.split("/");
        dayReport = tempdateReporte[0];
        monthReport = tempdateReporte[1];
        yearReport = tempdateReporte[2];
      }

      if (('' + dayReport).length == 1) {
        dayReport = '0' + dayReport;
      } else {
        dayReport = dayReport + '';
      }

      //var monthReport = tempdateReporte.getMonth() + 1;

      if (('' + monthReport).length == 1) {
        monthReport = '0' + monthReport;
      } else {
        monthReport = monthReport + '';
      }

      //var yearReport = tempdateReporte.getFullYear();

      periodoDate = dayReport + '/' + monthReport + '/' + yearReport;

      return periodoDate;
    }

    function obtenerPeriodoAdjustment() {
      var paramPeriodAdjust;
      var firstDay;
      var lastDay;

      var columnFrom = search.lookupFields({
        type: 'accountingperiod',
        id: paramperiodo,
        columns: ['enddate', 'startdate']
      });
      firstDay = columnFrom.startdate;
      lastDay = columnFrom.enddate;

      var savedsearch = search.create({
        type: "accountingperiod",
        filters: [
          ["isinactive", "is", "F"],
          "AND",
          ["isadjust", "is", "T"],
          "AND",
          ["startdate", "onorafter", firstDay],
          "AND",
          ["enddate", "onorbefore", lastDay]
        ],
        columns: [
          search.createColumn({
            name: "periodname",
            summary: "GROUP",
            sort: search.Sort.ASC,
            label: "Name"
          }),
          search.createColumn({
            name: "internalid",
            summary: "GROUP",
            label: "Internal ID"
          })
        ]
      });
      var pagedData = savedsearch.runPaged({
        pageSize: 1000
      });

      pagedData.pageRanges.forEach(function (pageRange) {
        page = pagedData.fetch({
          index: pageRange.index
        });

        page.data.forEach(function (result) {
          columns = result.columns;
          paramPeriodAdjust = result.getValue(columns[1]);
        })
      });

      return paramPeriodAdjust;
    }

    function ValidarAcentos(s) {
      var AccChars = "ŠŽšžŸÀÁÂÃÄÅÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖÙÚÛÜÝàáâãäåçèéêëìíîïðñòóôõöùúûüýÿ&°–—ªº·";
      var RegChars = "SZszYAAAAAACEEEEIIIIDNOOOOOUUUUYaaaaaaceeeeiiiidnooooouuuuyyyo--ao.";
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

    function getGlobalLabels() {
      var labels = {
        "Alert1": {
          "es": "LIBRO MAYOR Y BALANCE",
          "en": "LEDGER AND BALANCE",
          "pt": "LIVRO RAZÃO E EQUILÍBRIO"
        },
        "Alert2": {
          "es": "Razon Social: ",
          "en": "Company Name: ",
          "pt": "Razão Social: "
        },
        "Alert3": {
          "es": "Periodo: ",
          "en": "Period: ",
          "pt": "Período: "
        },
        "Alert4": {
          "es": "Libro Contable: ",
          "en": "Accounting Book: ",
          "pt": "Livro de Contabilidade: "
        },
        "Alert5": {
          "es": "Cuenta",
          "en": "Account",
          "pt": "Conta"
        },
        "Alert6": {
          "es": "Denominación",
          "en": "Denomination",
          "pt": "Denominação"
        },
        "Alert7": {
          "es": "Saldo Anterior",
          "en": "Previous Balance",
          "pt": "Saldo Anterior"
        },
        "Alert8": {
          "es": "Movimiento",
          "en": "Movement",
          "pt": "Movimento"
        },
        "Alert9": {
          "es": "Nuevo Saldo",
          "en": "New Balance",
          "pt": "Novo balanço"
        },
        "Alert10": {
          "es": "Debe",
          "en": "Debit",
          "pt": "Débito"
        },
        "Alert11": {
          "es": "Haber",
          "en": "Credit",
          "pt": "Crédito"
        },
        "Alert12": {
          "es": "No existe informacion para los criterios seleccionados",
          "en": "There is no information for the selected criteria",
          "pt": "Não há informações para os critérios selecionados"
        },
        "Alert13": {
          "es": "TOTALES",
          "en": "TOTALS",
          "pt": "TOTAIS"
        },
        "Alert14": {
          "es": "Documento",
          "en": "Document",
          "pt": "Documento"
        },
        "origin": {
          "es": "Origen: ",
          "en": "Origin: ",
          "pt": "Origem: "
        },
        "date": {
          "es": "Fecha: ",
          "en": "Date: ",
          "pt": "Data: "
        },
        "time": {
          "es": "Hora: ",
          "en": "Time: ",
          "pt": "Hora: "
        },
        "page": {
          "es": "Página",
          "en": "Page",
          "pt": "Página"
        },
        "of": {
          "es": "de",
          "en": "of",
          "pt": "de"
        }
      };

      return labels;
    }

    function getRangeDate(inicio, fin) {
      var arr = [];

      var savedSearch = search.create({
        type: search.Type.ACCOUNTING_PERIOD,
        filters: [
          ["isinactive", "is", "F"],
          "AND", ["isquarter", "is", "F"],
          "AND", ["isyear", "is", "F"],
          "AND", ["startdate", "onorafter", inicio],
          "AND", ["startdate", "before", fin]
        ],
        columns: [
          search.createColumn({
            name: "periodname",
            sort: search.Sort.ASC,
            label: "Name"
          }),
          search.createColumn({
            name: "internalid",
            label: "Internal ID"
          }),
        ]
      });

      var searchResult = savedSearch.run().getRange(0, 100);
      for (var index = 0; index < searchResult.length; index++) {
        var columns = searchResult[index].columns;
        arr.push(searchResult[index].getValue(columns[1]))
      }

      return arr;
    }

    function ObtenerFormulaFiltroPeriodo(arr, type) {
      var formula = '';
      if (type == 1) {
        var formula = 'CASE WHEN ';

        for (var i = 0; i < arr.length; i++) {
          formula += "{postingperiod.id} = '" + arr[i] + "'";
          if (i != arr.length - 1) {
            formula += ' OR ';
          }
        }

        formula += ' THEN 1 ELSE 0 END';
      } else {
        var formula = 'CASE WHEN ';
        for (var i = 0; i < arr.length; i++) {
          formula += "{transaction.postingperiod.id} = '" + arr[i] + "'";
          if (i != arr.length - 1) {
            formula += ' OR ';
          }
        }

        formula += ' THEN 1 ELSE 0 END';
      }

      return formula;
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
      execute: execute
    };
  });