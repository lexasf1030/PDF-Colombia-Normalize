/* = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =\
||   This script for customer center (Time)                     ||
||                                                              ||
||  File Name: LMRY_CO_BalCompTerceros_SCHDL_v2.0.js            ||
||                                                              ||
||  Version Date         Author        Remarks                  ||
||  2.0     Jun 18 2018  LatamReady    Use Script 2.0           ||
\= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = */
/**
 * @NApiVersion 2.0
 * @NScriptType ScheduledScript
 * @NModuleScope Public
 */
define(["N/record", "N/runtime", "N/file", "N/email", "N/search",
    "N/log", "N/config", "N/task", "N/encode", "N/format", "./CO_Library_Mensual/LMRY_CO_Reportes_LBRY_V2.0.js", "/SuiteBundles/Bundle 37714/Latam_Library/LMRY_libSendingEmailsLBRY_V2.0.js"
  ],

  function(recordModulo, runtime, fileModulo, email, search, log,
    config, task, encode, format, libreria, libFeature) {

    /**
    * @type {{entityId:string,subsidiaryId:string,multibookId:string,recordId:string|null,lastPucIndex:string,period:{initial:string,final:string,isAdjustment:boolean},isOpenBalance:boolean,isCopuc8:boolean,fileId:string|undefined,step:string|undefined}}
    */
    var params = {
        entityId: '',
        subsidiaryId: '',
        multibookId: '',
        recordId: null,
        lastPucIndex: '',
        period: {
            initial: '',
            final: '',
            isAdjustment: false
        },
        isOpenBalance: false,
        isCopuc8: false,
        fileId: undefined,
        step: undefined
    };
    /**
    * @type {{entityId:string,subsidiaryId:string,multibookId:string,recordId:string|null,lastPucIndex:string,period:{initial:string,final:string,isAdjustment:boolean},isOpenBalance:boolean,isCopuc8:boolean,fileId:string|undefined,step:string|undefined}}
    */
    var paramsAux = {
        entityId: '',
        subsidiaryId: '',
        multibookId: '',
        recordId: null,
        lastPucIndex: '',
        period: {
            initial: '',
            final: '',
            isAdjustment: false
        },
        isOpenBalance: false,
        isCopuc8: false,
        fileId: undefined,
        step: undefined
    };
    /**
    * @type {{multibookId:string|null,recordId:string|null,subsidiaryId:string|null}}
    */
    var paramsCommonAux = {
        multibookId: null,
        recordId: null,
        subsidiaryId: null
    };
    /**
    /**
    * @type {{entityId:string|null,lastPucIndex:string|null,initialPeriod:string|null,finalPeriod:string|null,isAdjustment:boolean,isOpenBalance:boolean,isCopuc8:boolean}}
    */
    var paramsOthersAux = {
        entityId: null,
        lastPucIndex: null,
        initialPeriod: null,
        finalPeriod: null,
        isAdjustment: false,
        isOpenBalance: false,
        isCopuc8: false
    };

    var objContext = runtime.getCurrentScript();
    // Nombre del Reporte
    var namereport = 'CO - Balance de Comprobacion por Terceros';
    var LMRY_script = 'LMRY_CO_BalCompTerceros_SCHDL_v2.0.js';

    var paramRecordID = null;
    var paramFileID = null;
    var paramEntityID = null;
    var paramMultibook = null;
    var paramSubsidy = null;
    var paramPeriod = null;
    var paramStep = null;
    var paramLastPuc = null;
    var paramPeriodFin = null;
    var paramAdjustment = null;
    var paramOpenBalance = null
    var paramPuc8D = null;

    //Features
    var featuresubs = runtime.isFeatureInEffect({
      feature: "SUBSIDIARIES"
    });
    var feamultibook = runtime.isFeatureInEffect({
      feature: "MULTIBOOK"
    });
    var featureCalendars = runtime.isFeatureInEffect({
      feature: "MULTIPLECALENDARS"
    });

    var featAccountingSpecial;
    var calendarSubsi;

    var ArrData = new Array();
    var ArrAccounts = new Array();

    var entity_name;
    var multibookName;

    var companyruc;
    var companyname;

    var periodenddate;
    var periodstartdate;
    var periodname;
    var periodnamefinal;
    var periodnameLog;
    var periodnamefinalLog;

    var flagEmpty = false;

    var entityCustomer = false;
    var entityVendor = false;
    var entityEmployee = false;

    var entity_name;
    var entity_id;
    var entity_nit;

    //PDF Normalization
    var todays = "";
    var currentTime = "";

    var GLOBAL_LABELS = {};
    var language = runtime.getCurrentScript().getParameter({
      name: 'LANGUAGE'
    }).substring(0, 2);

    function execute(context) {
      try {
        ObtenerParametros();
        GLOBAL_LABELS = getGlobalLabels();

        if (paramStep == 0) {
          ArrAccounts = ObtenerCuentas();

          if (paramFileID != null) {
            var strFile = ObtenerFile();

            ArrData = ConvertToArray(strFile);
          }

          if (ArrData.length != 0) {
            if (paramPuc8D) {
              ArrData = CambiarDataCuentaPuc8d(ArrData);
            } else {
              ArrData = CambiarDataCuenta(ArrData);
            }
            paramFileID = saveTemporal(ArrData, 'DATA_CON_CUENTAS_PUC.txt');
            paramStep++;
            LlamarSchedule();

          } else {
            NoData();

            paramLastPuc++;

            if (paramLastPuc < 9) {
              LlamarMapReduce();
            }

            return true;
          }

        } else if (paramStep == 1) {
          if (paramFileID != null) {
            var strFile = ObtenerFile();
            ArrData = ConvertToArray(strFile);
          }

          ArrData.sort(sortFunction);

          function sortFunction(a, b) {
            if (a[0] === b[0]) {
              return 0;
            } else {
              return (a[0] < b[0]) ? -1 : 1;
            }
          }

          paramFileID = saveTemporal(ArrData, 'ORDENADITO_POR_PUCS.txt');
          paramStep++;
          LlamarSchedule();

        } else if (paramStep == 2) {
          if (paramFileID != null) {
            var strFile = ObtenerFile();
            ArrData = ConvertToArray(strFile);
          }

          if (paramPuc8D) {
            ArrData = AgruparPorPucsYEntidadPuc8d(ArrData);
          } else {
            ArrData = AgruparPorPucsYEntidad(ArrData);
          }

          paramFileID = saveTemporal(ArrData, 'AgrupadoPUCSYENTIDAD.txt');
          paramStep++;
          LlamarSchedule();

        } else if (paramStep == 3) {
          if (paramFileID != null) {
            var strFile = ObtenerFile();
            ArrData = ConvertToArray(strFile);
          }

          if (paramPuc8D) {
            ArrData = AgregarArregloSeisDigitos(ArrData);
          } else {
            ArrData = AgregarArregloCuatroDigitos(ArrData);
          }

          paramFileID = saveTemporal(ArrData, 'TERCEROS_ORDENADOS_PUC.txt');
          paramStep++;
          LlamarSchedule();

        } else if (paramStep == 4) {
          if (paramFileID != null) {
            var strFile = ObtenerFile();
            ArrData = ConvertToArray(strFile);
          }

          if (ArrData.length == 0) {
            flagEmpty = true;
          }

          todays = parseDateTo(new Date(), "DATE");
          currentTime = getTimeHardcoded(parseDateTo(new Date(), "DATETIME"));

          if (paramPuc8D) {
            GeneraArchivoPuc8d(ArrData);
          } else {
            GeneraArchivo(ArrData);
          }
          
          paramLastPuc++;

          if (paramLastPuc < 9) {
            //LlamarSchedule();
            LlamarMapReduce();
          }
        }
      } catch (err) {
        log.error('err', err);
        libreria.sendMail(namereport, ' [ execute ] ' + err);
      }
    }

    function getGlobalLabels() {
      var labels = {
        "titulo": {
          "es": 'LIBRO DE BALANCE DE COMPROBACION POR TERCEROS',
          "pt": 'LIVRO DE BALANÇO DE TERCEIROS',
          "en": 'THIRD PARTY BALANCE SHEET BOOK'
        },
        "razonSocial": {
          "es": 'Razon Social',
          "pt": 'Razão social',
          "en": 'Business name'
        },
        "periodo": {
          "es": 'Periodo',
          "pt": 'Período',
          "en": 'Period'
        },
        "cuenta": {
          "es": 'Cuenta',
          "pt": 'Conta',
          "en": 'Account'
        },
        "denominacion": {
          "es": 'Denominación',
          "pt": 'Denominação',
          "en": 'Denomination'
        },
        "entidad": {
          "es": 'Entidad',
          "pt": 'Entidade',
          "en": 'Entity'
        },
        "debe": {
          "es": 'Debe',
          "pt": 'Débito',
          "en": 'Debit'
        },
        "haber": {
          "es": 'Haber',
          "pt": 'Crédito',
          "en": 'Credit'
        },
        "saldoAnterior": {
          "es": 'Saldo Anterior',
          "pt": 'Saldo Anterior',
          "en": 'Previous balance'
        },
        "movimiento": {
          "es": 'Movimiento',
          "pt": 'Movimento',
          "en": 'Movement'
        },
        "nuevoSaldo": {
          "es": 'Nuevo Saldo',
          "pt": 'Novo Balanço',
          "en": 'New Balance'
        },
        'nodata': {
          "es": 'No existe informacion para los criterios seleccionados',
          "pt": 'Não há informações para os critérios selecionados',
          "en": 'There is no information for the selected criteria'
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
        }
      }

      return labels;
    }

    function LlamarMapReduce() {
      try {
        paramsOthersAux.initialPeriod = paramPeriod;

        if (paramPeriodFin != null) {
          paramsOthersAux.finalPeriod = paramPeriodFin;
        }
        if (featuresubs) {
          paramsCommonAux.subsidiaryId = paramSubsidy;
        }
        if (feamultibook) {
          paramsCommonAux.multibookId = paramMultibook;
        }
        if (paramEntityID != null) {
            paramsOthersAux.entityId = paramEntityID;
        }
        if (paramRecordID != null) {
            paramsCommonAux.recordId = paramRecordID;
        }

        paramsOthersAux.lastPucIndex = paramLastPuc;
        paramsOthersAux.isAdjustment = paramAdjustment;
        paramsOthersAux.isOpenBalance = paramOpenBalance;
        paramsOthersAux.isCopuc8 = paramPuc8D;

        log.debug("PARAMS",{  paramsOthersAux: paramsOthersAux , paramsCommonAux: paramsCommonAux });

        var mprdcParams = {};
        mprdcParams['custscript_lmry_co_terc_mprdc_globals'] = paramsCommonAux;
        mprdcParams['custscript_lmry_co_terc_mprdc_others'] = paramsOthersAux;

        var RedirecSchdl = task.create({
          taskType: task.TaskType.MAP_REDUCE,
          scriptId: 'customscript_lmry_co_bcmp_terc_mprd_v2_0',
          deploymentId: 'customdeploy_lmry_co_bcmp_terc_mprd_v2_0',
          params: mprdcParams
        });

        RedirecSchdl.submit();
      } catch (err) {
        log.error('err', err);
      }
    }

    function AgruparPorPucsYEntidad(ArrTemp) {
      var ArrReturn = new Array();

      ArrReturn.push(ArrTemp[0]);

      for (var i = 1; i < ArrTemp.length; i++) {
        if (ArrTemp[i][0].trim() != '') {
          var intLength = ArrReturn.length;
          for (var j = 0; j < intLength; j++) {
            if (ArrTemp[i][0] == ArrReturn[j][0] && ArrTemp[i][14].trim() == ArrReturn[j][14].trim()) {
              ArrReturn[j][2] = Math.abs(ArrReturn[j][2]) + Math.abs(ArrTemp[i][2]);
              ArrReturn[j][3] = Math.abs(ArrReturn[j][3]) + Math.abs(ArrTemp[i][3]);
              ArrReturn[j][4] = Math.abs(ArrReturn[j][4]) + Math.abs(ArrTemp[i][4]);
              ArrReturn[j][5] = Math.abs(ArrReturn[j][5]) + Math.abs(ArrTemp[i][5]);
              ArrReturn[j][6] = Math.abs(ArrReturn[j][6]) + Math.abs(ArrTemp[i][6]);
              ArrReturn[j][7] = Math.abs(ArrReturn[j][7]) + Math.abs(ArrTemp[i][7]);
              break;
            }
            if (j == ArrReturn.length - 1) {
              ArrReturn.push(ArrTemp[i]);
            }
          }
        }
      }

      return ArrReturn;
    }

    function AgruparPorPucsYEntidadPuc8d(ArrTemp) {
      var ArrReturn = new Array();

      ArrReturn.push(ArrTemp[0]);

      for (var i = 1; i < ArrTemp.length; i++) {
        if (ArrTemp[i][0].trim() != '') {
          var intLength = ArrReturn.length;
          for (var j = 0; j < intLength; j++) {
            if (ArrTemp[i][0] == ArrReturn[j][0] && ArrTemp[i][16].trim() == ArrReturn[j][16].trim()) {
              ArrReturn[j][2] = Math.abs(ArrReturn[j][2]) + Math.abs(ArrTemp[i][2]);
              ArrReturn[j][3] = Math.abs(ArrReturn[j][3]) + Math.abs(ArrTemp[i][3]);
              ArrReturn[j][4] = Math.abs(ArrReturn[j][4]) + Math.abs(ArrTemp[i][4]);
              ArrReturn[j][5] = Math.abs(ArrReturn[j][5]) + Math.abs(ArrTemp[i][5]);
              ArrReturn[j][6] = Math.abs(ArrReturn[j][6]) + Math.abs(ArrTemp[i][6]);
              ArrReturn[j][7] = Math.abs(ArrReturn[j][7]) + Math.abs(ArrTemp[i][7]);
              break;
            }
            if (j == ArrReturn.length - 1) {
              ArrReturn.push(ArrTemp[i]);
            }
          }
        }
      }

      return ArrReturn;
    }

    function LlamarSchedule() {
      paramsAux.period.initial = paramPeriod;
      paramsAux.fileId = paramFileID;
      paramsAux.step = Number(paramStep);
      paramsAux.lastPucIndex = paramLastPuc;
      paramsAux.period.isAdjustment = paramAdjustment;
      paramsAux.isOpenBalance = paramOpenBalance;
      paramsAux.isCopuc8 = paramPuc8D;
      
      if (featuresubs) {
        paramsAux.subsidiaryId = paramSubsidy;
      }
      if (feamultibook) {
        paramsAux.multibookId = paramMultibook;
      }
      if (paramEntityID != null) {
        paramsAux.entityId = paramEntityID;
      }
      if (paramRecordID != null) {
        paramsAux.recordId = paramRecordID;
      }
      if (paramPeriodFin != null) {
        paramsAux.period.final = paramPeriodFin;
      }

      log.debug('paramsAux', { paramsAux: paramsAux });

      var schdlParams = {};
      schdlParams['custscript_lmry_co_terc_schdl'] = paramsAux;

      var RedirecSchdl = task.create({
        taskType: task.TaskType.SCHEDULED_SCRIPT,
        scriptId: 'customscript_lmry_co_bcmp_ter_schdl_v2_0',
        deploymentId: 'customdeploy_lmry_co_bcmp_ter_schdl_v2_0',
        params: schdlParams
      });
      RedirecSchdl.submit();
    }

    function saveTemporal(arrTemp, Final_NameFile) {
      var strTemp = '';

      for (var i = 0; i < arrTemp.length; i++) {
        for (var j = 0; j < arrTemp[i].length; j++) {
          strTemp += arrTemp[i][j];
          strTemp += '|';
        }
        strTemp += '\r\n';
      }

      var FolderId = objContext.getParameter({
        name: 'custscript_lmry_file_cabinet_rg_co'
      });

      // Almacena en la carpeta de Archivos Generados
      if (FolderId != '' && FolderId != null) {
        // Crea el archivo.xls
        var file = fileModulo.create({
          name: Final_NameFile,
          fileType: fileModulo.Type.PLAINTEXT,
          contents: strTemp,
          folder: FolderId
        });

        var idfile = file.save();
      }

      return idfile;
    }

    function ObtenerDatosSubsidiaria() {
      var configpage = config.load({
        type: config.Type.COMPANY_INFORMATION
      });

      if (featuresubs) {

        var subsidiaryData = search.lookupFields({
          type: search.Type.SUBSIDIARY,
          id: paramSubsidy,
          columns: ['legalname', 'taxidnum']
        });
        companyname = subsidiaryData.legalname;
        companyruc = subsidiaryData.taxidnum;

        if (featureCalendars || featureCalendars == 'T') {
          var subsidiary = search.lookupFields({
            type: search.Type.SUBSIDIARY,
            id: paramSubsidy,
            columns: ['fiscalcalendar']
          });

          calendarSubsi = subsidiary.fiscalcalendar[0].value;
          log.debug('calendarSubsi', calendarSubsi);
        }

        // var licenses = libFeature.getLicenses(paramSubsidy);
        // featAccountingSpecial = libFeature.getAuthorization(677, licenses);
        log.debug('featAccountingSpecial', featAccountingSpecial);

      } else {
        companyruc = configpage.getValue('employerid');
        companyname = configpage.getValue('legalname');
      }

      companyruc = companyruc.replace(' ', '');
    }

    function GeneraArchivoPuc8d(ArrSaldoFinal) {
      var DbolStop = false;

      var flagAllZero = true;

      if (ArrSaldoFinal.length != null && ArrSaldoFinal.length != 0 && !flagEmpty) {

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

        //Cabecera
        var xlsCabecera = '';
        xlsCabecera = '<Row>';
        xlsCabecera += '<Cell></Cell>';
        xlsCabecera += '<Cell></Cell>';
        xlsCabecera += '<Cell></Cell>';
        xlsCabecera += '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['titulo'][language] + ' </Data></Cell>';
        xlsCabecera += '</Row>';
        xlsCabecera += '<Row></Row>';
        xlsCabecera += '<Row>';
        xlsCabecera += '<Cell></Cell>';
        xlsCabecera += '<Cell></Cell>';
        xlsCabecera += '<Cell  ss:StyleID="s22"><Data ss:Type="String">' + GLOBAL_LABELS['razonSocial'][language] + ': ' + companyname + '</Data></Cell>';
        xlsCabecera += '</Row>';
        xlsCabecera += '<Row>';
        xlsCabecera += '<Cell></Cell>';
        xlsCabecera += '<Cell></Cell>';
        xlsCabecera += '<Cell  ss:StyleID="s22"><Data ss:Type="String">NIT: ' + companyruc + '</Data></Cell>';
        xlsCabecera += '</Row>';
        xlsCabecera += '<Row>';
        xlsCabecera += '<Cell></Cell>';
        xlsCabecera += '<Cell></Cell>';
        if (paramPeriodFin != null && paramPeriodFin != '') {
          xlsCabecera += '<Cell  ss:StyleID="s22"><Data ss:Type="String">' + GLOBAL_LABELS['periodo'][language] + ': ' + periodname + ' - ' + periodnamefinal + '</Data></Cell>';
        } else {
          xlsCabecera += '<Cell  ss:StyleID="s22"><Data ss:Type="String">' + GLOBAL_LABELS['periodo'][language] + ': ' + periodstartdate + ' - ' + periodenddate + '</Data></Cell>';
        }
        xlsCabecera += '</Row>';
        if ((feamultibook || feamultibook == 'T') && (paramMultibook != '' && paramMultibook != null)) {
          xlsCabecera += '<Row>';
          xlsCabecera += '<Cell></Cell>';
          xlsCabecera += '<Cell></Cell>';
          xlsCabecera += '<Cell ss:StyleID="s22"><Data ss:Type="String">Multibooking: ' + multibookName + '</Data></Cell>';
          xlsCabecera += '</Row>';
        }

        if (paramEntityID != null && paramEntityID != '') {
          var flag_entity = ObtenerEntidad(paramEntityID);
          var name_enti = ValidarAcentos(entity_name);
          if (flag_entity) {
            xlsCabecera += '<Row>';
            xlsCabecera += '<Cell></Cell>';
            xlsCabecera += '<Cell></Cell>';
            xlsCabecera += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + GLOBAL_LABELS['entidad'][language] + ': ' + name_enti + '</Data></Cell>';
            xlsCabecera += '</Row>';
          }
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
          '<Cell></Cell>' +
          '<Cell ss:StyleID="s21"><Data ss:Type="String">' + GLOBAL_LABELS['saldoAnterior'][language] + '</Data></Cell>' +
          '<Cell></Cell>' +
          '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['movimiento'][language] + ' </Data></Cell>' +
          '<Cell></Cell>' +
          '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['nuevoSaldo'][language] + ' </Data></Cell>' +
          '</Row>';

        xlsCabecera += '<Row>' +
          '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['cuenta'][language] + ' </Data></Cell>' +
          '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['denominacion'][language] + ' </Data></Cell>' +
          '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['entidad'][language] + ' </Data></Cell>' +
          '<Cell ss:StyleID="s21"><Data ss:Type="String"> NIT </Data></Cell>' +
          '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['debe'][language] + ' </Data></Cell>' +
          '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['haber'][language] + ' </Data></Cell>' +
          '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['debe'][language] + ' </Data></Cell>' +
          '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['haber'][language] + ' </Data></Cell>' +
          '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['debe'][language] + ' </Data></Cell>' +
          '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['haber'][language] + ' </Data></Cell>' +
          '</Row>';

        var xlsString = xlsArchivo + xlsCabecera;

        var _TotalSIDebe = 0.0;
        var _TotalSIHaber = 0.0;
        var _TotalMovDebe = 0.0;
        var _TotalMovHaber = 0.0;
        var _TotalSFDebe = 0.0;
        var _TotalSFHaber = 0.0;

        // Formato de la celda
        _StyleTxt = ' ss:StyleID="s22" ';
        _StyleNum = ' ss:StyleID="s23" ';

        for (var i = 0; i < ArrSaldoFinal.length; i++) {
          if (Math.abs(ArrSaldoFinal[i][2]) == 0 && Math.abs(ArrSaldoFinal[i][3]) == 0 &&
            Math.abs(ArrSaldoFinal[i][4]) == 0 && Math.abs(ArrSaldoFinal[i][5]) == 0 &&
            Math.abs(ArrSaldoFinal[i][6]) == 0 && Math.abs(ArrSaldoFinal[i][7]) == 0) {

          } else {
            if (ArrSaldoFinal[i][0].charAt(0) == paramLastPuc) {

              flagAllZero = false;

              xlsString += '<Row>';

              //////////////////////////////////
              //numero de cuenta
              if (ArrSaldoFinal[i][0] != '' && ArrSaldoFinal[i][0] != null && ArrSaldoFinal[i][0] != '- None -') {

                xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + ArrSaldoFinal[i][0] + '</Data></Cell>';
              } else {
                xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String"></Data></Cell>';
              }

              //Denominacion
              if (ArrSaldoFinal[i][1].length > 0 && ArrSaldoFinal[i][0] != '- None -') {
                var s = ValidarAcentos(ArrSaldoFinal[i][1]);
                xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + s + '</Data></Cell>';
              } else {
                xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String"></Data></Cell>';
              }
              //Entidad
              if (ArrSaldoFinal[i][16] != null && ArrSaldoFinal[i][16] != '' && ArrSaldoFinal[i][16] != '- None -') {
                //log.debug('ArrSaldoFinal[i][16]',ArrSaldoFinal[i][16]);
                var entidad = ArrSaldoFinal[i][16];
                var json_entity = JSON.parse(entidad);

                var nombre = '';

                if (json_entity != null) {
                  nombre = ValidarAcentos(json_entity.name);
                }

                xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + nombre + '</Data></Cell>';
              } else {
                xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String"></Data></Cell>';
              }

              //Entidad
              if (ArrSaldoFinal[i][16] != null && ArrSaldoFinal[i][16] != '' && ArrSaldoFinal[i][16] != '- None -') {
                var json_entity = JSON.parse(ArrSaldoFinal[i][16]);

                var nit = '';

                if (json_entity != null) {
                  var temp_nit = json_entity.nit;
                  nit = ((temp_nit).replace(/-/g, '')).replace(/_/g, '');
                }

                xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + nit + '</Data></Cell>';
              } else {
                xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String"></Data></Cell>';
              }

              /////////////////////////////////
              if (ArrSaldoFinal[i][0].length == 1 || ArrSaldoFinal[i][0].length == 2 || ArrSaldoFinal[i][0].length == 4 || ArrSaldoFinal[i][0].length == 6) {
                var saldo_antes = Math.abs(ArrSaldoFinal[i][2]) - Math.abs(ArrSaldoFinal[i][3]);
                var saldo_actual = Math.abs(ArrSaldoFinal[i][2]) + Math.abs(ArrSaldoFinal[i][4]) - Math.abs(ArrSaldoFinal[i][3]) - Math.abs(ArrSaldoFinal[i][5]);
                var movimientos = Math.abs(ArrSaldoFinal[i][4]) - Math.abs(ArrSaldoFinal[i][5]);

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

                if (movimientos < 0) {
                  //Haber Antes
                  xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + 0.0 + '</Data></Cell>';
                  //Debe Antes
                  xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(movimientos) + '</Data></Cell>';
                } else {
                  if (movimientos > 0) {
                    //Haber Antes
                    xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(movimientos) + '</Data></Cell>';
                    //Debe Antes
                    xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + 0.0 + '</Data></Cell>';
                  } else {
                    xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + 0.0 + '</Data></Cell>';
                    xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + 0.0 + '</Data></Cell>';

                  }
                }

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

                if (ArrSaldoFinal[i][0].length == 1) {
                  var a = Math.abs(ArrSaldoFinal[i][2]) - Math.abs(ArrSaldoFinal[i][3]);
                  var b = Math.abs(ArrSaldoFinal[i][4]) - Math.abs(ArrSaldoFinal[i][5]);
                  var c = Math.abs(ArrSaldoFinal[i][2]) - Math.abs(ArrSaldoFinal[i][3]) + Math.abs(ArrSaldoFinal[i][4]) - Math.abs(ArrSaldoFinal[i][5]);

                  if (a >= 0) {
                    _TotalSIDebe += a;
                  } else {
                    _TotalSIHaber += a;
                  }

                  if (b >= 0) {
                    _TotalMovDebe += b;
                  } else {
                    _TotalMovHaber += b;
                  }

                  if (c >= 0) {
                    _TotalSFDebe += c;
                  } else {
                    _TotalSFHaber += c;
                  }
                }

              } else {
                //Debe Antes
                xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(Number(ArrSaldoFinal[i][2])) + '</Data></Cell>';
                //Haber Antes
                xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(Number(ArrSaldoFinal[i][3])) + '</Data></Cell>';
                //Haber Movimientos
                xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(Number(ArrSaldoFinal[i][4])) + '</Data></Cell>';
                //Debe Movimientos
                xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(Number(ArrSaldoFinal[i][5])) + '</Data></Cell>';
                //Debe Saldo
                xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(Number(ArrSaldoFinal[i][6])) + '</Data></Cell>';
                //Haber Saldo
                xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(Number(ArrSaldoFinal[i][7])) + '</Data></Cell>';

              }
              xlsString += '</Row>';
            }
          }
        }

        xlsString += '<Row>';
        xlsString += '<Cell></Cell>';
        xlsString += '<Cell></Cell>';
        xlsString += '<Cell ' + _StyleTxt + '><Data ss:Type="String">TOTALES</Data></Cell>';
        xlsString += '<Cell></Cell>';
        xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(_TotalSIDebe) + '</Data></Cell>';
        xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(_TotalSIHaber) + '</Data></Cell>';
        xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(_TotalMovDebe) + '</Data></Cell>';
        xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(_TotalMovHaber) + '</Data></Cell>';
        xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(_TotalSFDebe) + '</Data></Cell>';
        xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(_TotalSFHaber) + '</Data></Cell>';
        xlsString += '</Row>';

        xlsString += '</Table></Worksheet></Workbook>';

        // Se arma el archivo EXCEL
        Final_string = encode.convert({
          string: xlsString,
          inputEncoding: encode.Encoding.UTF_8,
          outputEncoding: encode.Encoding.BASE_64
        });

        if (flagAllZero) {
          NoData();
        } else {
          savefile(Final_string);
        }

        paramRecordID = null;
      } else {
        NoData();
      }
    }

    function GeneraArchivo(ArrSaldoFinal) {
      var DbolStop = false;

      var flagAllZero = true;

      if (ArrSaldoFinal.length != null && ArrSaldoFinal.length != 0 && !flagEmpty) {

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

        //Cabecera
        var xlsCabecera = '';
        xlsCabecera = '<Row>';
        xlsCabecera += '<Cell></Cell>';
        xlsCabecera += '<Cell></Cell>';
        xlsCabecera += '<Cell></Cell>';
        xlsCabecera += '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['titulo'][language] + ' </Data></Cell>';
        xlsCabecera += '</Row>';
        xlsCabecera += '<Row></Row>';
        xlsCabecera += '<Row>';
        xlsCabecera += '<Cell></Cell>';
        xlsCabecera += '<Cell></Cell>';
        xlsCabecera += '<Cell  ss:StyleID="s22"><Data ss:Type="String">' + GLOBAL_LABELS['razonSocial'][language] + ': ' + companyname + '</Data></Cell>';
        xlsCabecera += '</Row>';
        xlsCabecera += '<Row>';
        xlsCabecera += '<Cell></Cell>';
        xlsCabecera += '<Cell></Cell>';
        xlsCabecera += '<Cell  ss:StyleID="s22"><Data ss:Type="String">NIT: ' + companyruc + '</Data></Cell>';
        xlsCabecera += '</Row>';
        xlsCabecera += '<Row>';
        xlsCabecera += '<Cell></Cell>';
        xlsCabecera += '<Cell></Cell>';
        if (paramPeriodFin != null && paramPeriodFin != '') {
          xlsCabecera += '<Cell  ss:StyleID="s22"><Data ss:Type="String">' + GLOBAL_LABELS['periodo'][language] + ': ' + periodname + ' - ' + periodnamefinal + '</Data></Cell>';
        } else {
          xlsCabecera += '<Cell  ss:StyleID="s22"><Data ss:Type="String">' + GLOBAL_LABELS['periodo'][language] + ': ' + periodstartdate + ' - ' + periodenddate + '</Data></Cell>';
        }
        xlsCabecera += '</Row>';
        if ((feamultibook || feamultibook == 'T') && (paramMultibook != '' && paramMultibook != null)) {
          xlsCabecera += '<Row>';
          xlsCabecera += '<Cell></Cell>';
          xlsCabecera += '<Cell></Cell>';
          xlsCabecera += '<Cell ss:StyleID="s22"><Data ss:Type="String">Multibooking: ' + multibookName + '</Data></Cell>';
          xlsCabecera += '</Row>';
        }

        if (paramEntityID != null && paramEntityID != '') {
          var flag_entity = ObtenerEntidad(paramEntityID);
          var name_enti = ValidarAcentos(entity_name);
          if (flag_entity) {
            xlsCabecera += '<Row>';
            xlsCabecera += '<Cell></Cell>';
            xlsCabecera += '<Cell></Cell>';
            xlsCabecera += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + GLOBAL_LABELS['entidad'][language] + ': ' + name_enti + '</Data></Cell>';
            xlsCabecera += '</Row>';
          }
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
          '<Cell></Cell>' +
          '<Cell ss:StyleID="s21"><Data ss:Type="String">' + GLOBAL_LABELS['saldoAnterior'][language] + '</Data></Cell>' +
          '<Cell></Cell>' +
          '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['movimiento'][language] + ' </Data></Cell>' +
          '<Cell></Cell>' +
          '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['nuevoSaldo'][language] + ' </Data></Cell>' +
          '</Row>';

        xlsCabecera += '<Row>' +
          '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['cuenta'][language] + ' </Data></Cell>' +
          '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['denominacion'][language] + ' </Data></Cell>' +
          '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['entidad'][language] + ' </Data></Cell>' +
          '<Cell ss:StyleID="s21"><Data ss:Type="String"> NIT </Data></Cell>' +
          '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['debe'][language] + ' </Data></Cell>' +
          '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['haber'][language] + ' </Data></Cell>' +
          '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['debe'][language] + ' </Data></Cell>' +
          '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['haber'][language] + ' </Data></Cell>' +
          '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['debe'][language] + ' </Data></Cell>' +
          '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['haber'][language] + ' </Data></Cell>' +
          '</Row>';

        var xlsString = xlsArchivo + xlsCabecera;

        var _TotalSIDebe = 0.0;
        var _TotalSIHaber = 0.0;
        var _TotalMovDebe = 0.0;
        var _TotalMovHaber = 0.0;
        var _TotalSFDebe = 0.0;
        var _TotalSFHaber = 0.0;

        // Formato de la celda
        _StyleTxt = ' ss:StyleID="s22" ';
        _StyleNum = ' ss:StyleID="s23" ';

        for (var i = 0; i < ArrSaldoFinal.length; i++) {
          if (Math.abs(ArrSaldoFinal[i][2]) == 0 && Math.abs(ArrSaldoFinal[i][3]) == 0 &&
            Math.abs(ArrSaldoFinal[i][4]) == 0 && Math.abs(ArrSaldoFinal[i][5]) == 0 &&
            Math.abs(ArrSaldoFinal[i][6]) == 0 && Math.abs(ArrSaldoFinal[i][7]) == 0) {

          } else {
            if (ArrSaldoFinal[i][0].charAt(0) == paramLastPuc) {

              flagAllZero = false;

              xlsString += '<Row>';

              //////////////////////////////////
              //numero de cuenta
              if (ArrSaldoFinal[i][0] != '' && ArrSaldoFinal[i][0] != null && ArrSaldoFinal[i][0] != '- None -') {

                xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + ArrSaldoFinal[i][0] + '</Data></Cell>';
              } else {
                xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String"></Data></Cell>';
              }

              //Denominacion
              if (ArrSaldoFinal[i][1].length > 0 && ArrSaldoFinal[i][0] != '- None -') {
                var s = ValidarAcentos(ArrSaldoFinal[i][1]);
                xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + s + '</Data></Cell>';
              } else {
                xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String"></Data></Cell>';
              }
              //Entidad
              if (ArrSaldoFinal[i][14] != null && ArrSaldoFinal[i][14] != '' && ArrSaldoFinal[i][14] != '- None -') {
                //log.debug('ArrSaldoFinal[i][14]',ArrSaldoFinal[i][14]);
                var entidad = ArrSaldoFinal[i][14];
                var json_entity = JSON.parse(entidad);

                var nombre = '';

                if (json_entity != null) {
                  nombre = ValidarAcentos(json_entity.name);
                }

                xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + nombre + '</Data></Cell>';
              } else {
                xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String"></Data></Cell>';
              }

              //Entidad
              if (ArrSaldoFinal[i][14] != null && ArrSaldoFinal[i][14] != '' && ArrSaldoFinal[i][14] != '- None -') {
                var json_entity = JSON.parse(ArrSaldoFinal[i][14]);

                var nit = '';

                if (json_entity != null) {
                  var temp_nit = json_entity.nit;
                  nit = ((temp_nit).replace(/-/g, '')).replace(/_/g, '');
                }

                xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + nit + '</Data></Cell>';
              } else {
                xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String"></Data></Cell>';
              }

              /////////////////////////////////
              if (ArrSaldoFinal[i][0].length == 1 || ArrSaldoFinal[i][0].length == 2 || ArrSaldoFinal[i][0].length == 4) {
                var saldo_antes = Math.abs(ArrSaldoFinal[i][2]) - Math.abs(ArrSaldoFinal[i][3]);
                var saldo_actual = Math.abs(ArrSaldoFinal[i][2]) + Math.abs(ArrSaldoFinal[i][4]) - Math.abs(ArrSaldoFinal[i][3]) - Math.abs(ArrSaldoFinal[i][5]);
                var movimientos = Math.abs(ArrSaldoFinal[i][4]) - Math.abs(ArrSaldoFinal[i][5]);

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

                if (movimientos < 0) {
                  //Haber Antes
                  xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + 0.0 + '</Data></Cell>';
                  //Debe Antes
                  xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(movimientos) + '</Data></Cell>';
                } else {
                  if (movimientos > 0) {
                    //Haber Antes
                    xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(movimientos) + '</Data></Cell>';
                    //Debe Antes
                    xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + 0.0 + '</Data></Cell>';
                  } else {
                    xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + 0.0 + '</Data></Cell>';
                    xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + 0.0 + '</Data></Cell>';

                  }
                }

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

                if (ArrSaldoFinal[i][0].length == 1) {
                  var a = Math.abs(ArrSaldoFinal[i][2]) - Math.abs(ArrSaldoFinal[i][3]);
                  var b = Math.abs(ArrSaldoFinal[i][4]) - Math.abs(ArrSaldoFinal[i][5]);
                  var c = Math.abs(ArrSaldoFinal[i][2]) - Math.abs(ArrSaldoFinal[i][3]) + Math.abs(ArrSaldoFinal[i][4]) - Math.abs(ArrSaldoFinal[i][5]);

                  if (a >= 0) {
                    _TotalSIDebe += a;
                  } else {
                    _TotalSIHaber += a;
                  }

                  if (b >= 0) {
                    _TotalMovDebe += b;
                  } else {
                    _TotalMovHaber += b;
                  }

                  if (c >= 0) {
                    _TotalSFDebe += c;
                  } else {
                    _TotalSFHaber += c;
                  }
                }

              } else {
                //Debe Antes
                xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(Number(ArrSaldoFinal[i][2])) + '</Data></Cell>';
                //Haber Antes
                xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(Number(ArrSaldoFinal[i][3])) + '</Data></Cell>';
                //Haber Movimientos
                xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(Number(ArrSaldoFinal[i][4])) + '</Data></Cell>';
                //Debe Movimientos
                xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(Number(ArrSaldoFinal[i][5])) + '</Data></Cell>';
                //Debe Saldo
                xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(Number(ArrSaldoFinal[i][6])) + '</Data></Cell>';
                //Haber Saldo
                xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(Number(ArrSaldoFinal[i][7])) + '</Data></Cell>';

              }
              xlsString += '</Row>';
            }
          }
        }

        xlsString += '<Row>';
        xlsString += '<Cell></Cell>';
        xlsString += '<Cell></Cell>';
        xlsString += '<Cell ' + _StyleTxt + '><Data ss:Type="String">TOTALES</Data></Cell>';
        xlsString += '<Cell></Cell>';
        xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(_TotalSIDebe) + '</Data></Cell>';
        xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(_TotalSIHaber) + '</Data></Cell>';
        xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(_TotalMovDebe) + '</Data></Cell>';
        xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(_TotalMovHaber) + '</Data></Cell>';
        xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(_TotalSFDebe) + '</Data></Cell>';
        xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(_TotalSFHaber) + '</Data></Cell>';
        xlsString += '</Row>';

        xlsString += '</Table></Worksheet></Workbook>';

        // Se arma el archivo EXCEL
        Final_string = encode.convert({
          string: xlsString,
          inputEncoding: encode.Encoding.UTF_8,
          outputEncoding: encode.Encoding.BASE_64
        });

        if (flagAllZero) {
          NoData();
        } else {
          savefile(Final_string);
        }

        paramRecordID = null;
      } else {
        NoData();
      }
    }

    function NoData() {
      var usuarioTemp = runtime.getCurrentUser();
      var usuario = usuarioTemp.name;

      if (paramRecordID != null && paramRecordID != '') {
        var record = recordModulo.load({
          type: 'customrecord_lmry_co_rpt_generator_log',
          id: paramRecordID
        });

        paramRecordID = null;
      } else {
        var record = recordModulo.create({
          type: 'customrecord_lmry_co_rpt_generator_log'
        });
      }
      //Nombre de Archivo
      record.setValue({
        fieldId: 'custrecord_lmry_co_rg_name',
        value: GLOBAL_LABELS['nodata'][language]
      });
      //Nombre de Reporte
      record.setValue({
        fieldId: 'custrecord_lmry_co_rg_transaction',
        value: namereport
      });
      //Nombre de Subsidiaria
      record.setValue({
        fieldId: 'custrecord_lmry_co_rg_subsidiary',
        value: companyname
      });
      //Periodo
      if (periodnamefinalLog != null && periodnamefinalLog != '') {
        record.setValue({
          fieldId: 'custrecord_lmry_co_rg_postingperiod',
          value: periodnameLog + ' - ' + periodnamefinalLog
        });
      } else {
        record.setValue({
          fieldId: 'custrecord_lmry_co_rg_postingperiod',
          value: periodnameLog
        });
      }
      //Multibook
      if (feamultibook || feamultibook == 'T') {
        record.setValue({
          fieldId: 'custrecord_lmry_co_rg_multibook',
          value: multibookName
        });
      }
      if (paramEntityID != null) {
        record.setValue({
          fieldId: 'custrecord_lmry_co_rg_entity',
          value: entity_name
        });
      }
      //Creado Por
      record.setValue({
        fieldId: 'custrecord_lmry_co_rg_employee',
        value: usuario
      });

      record.save();
    }

    function AgregarArregloSeisDigitos(ArrSaldoFinal) {
      for (var y = 0; y < ArrSaldoFinal.length; y++) {
        /*if (ArrSaldoFinal[y][1] == '' || ArrSaldoFinal[y][1] == null) {
          ArrSaldoFinal[y][1] = 0.0;
        }*/

        if (ArrSaldoFinal[y][2] == '' || ArrSaldoFinal[y][2] == null) {
          ArrSaldoFinal[y][2] = 0.0;
        }

        if (ArrSaldoFinal[y][3] == '' || ArrSaldoFinal[y][3] == null) {
          ArrSaldoFinal[y][3] = 0.0;
        }

        if (ArrSaldoFinal[y][4] == '' || ArrSaldoFinal[y][4] == null) {
          ArrSaldoFinal[y][4] = 0.0;
        }

        if (ArrSaldoFinal[y][5] == '' || ArrSaldoFinal[y][5] == null) {
          ArrSaldoFinal[y][5] = 0.0;
        }

        if (ArrSaldoFinal[y][6] == '' || ArrSaldoFinal[y][6] == null) {
          ArrSaldoFinal[y][6] = 0.0;
        }
      }
      /*
       * ARRAY FINAL
       * 0.  cuenta 8
       * 1.  denominacion 8
       * 2.  debitos antes
       * 3.  creditos antes
       * 4.  debitos actual
       * 5.  creditos actual
       * 6.  nuevo saldo debitos
       * 7.  nuevo saldo creditos
       * 8.  cuenta 6 digitos
       * 9.  denominacion 6 digitos
       * 10. cuenta 4 digitos
       * 11. denominacion 4 digitos
       * 12. cuenta 2 digitos
       * 13. denominacion 2 digitos
       * 14. cuenta 1 digito
       * 15. denominacion 1 digito
       * 16. entity
       */

      var cuenta_aux = ArrSaldoFinal[0][8];

      var array_6_digitos = new Array();

      array_6_digitos[0] = cuenta_aux;
      array_6_digitos[1] = ArrSaldoFinal[0][9];
      array_6_digitos[2] = 0.0;
      array_6_digitos[3] = 0.0;
      array_6_digitos[4] = 0.0;
      array_6_digitos[5] = 0.0;
      array_6_digitos[6] = 0.0;
      array_6_digitos[7] = 0.0;
      array_6_digitos[8] = ArrSaldoFinal[0][8];
      array_6_digitos[9] = ArrSaldoFinal[0][9];
      array_6_digitos[10] = ArrSaldoFinal[0][10];
      array_6_digitos[11] = ArrSaldoFinal[0][11];
      array_6_digitos[12] = ArrSaldoFinal[0][12];
      array_6_digitos[13] = ArrSaldoFinal[0][13];
      array_6_digitos[14] = ArrSaldoFinal[0][14];
      array_6_digitos[15] = ArrSaldoFinal[0][15];
      array_6_digitos[16] = '';

      //Agregar al  del array
      ArrSaldoFinal.splice(0, 0, array_6_digitos);

      var array_cuentas = new Array();

      array_cuentas[0] = array_6_digitos;

      for (var i = 0; i < ArrSaldoFinal.length; i++) {
        if (ArrSaldoFinal[i][8] != cuenta_aux) {
          cuenta_aux = ArrSaldoFinal[i][8];
          var array_aux = new Array();

          array_aux[0] = cuenta_aux;
          array_aux[1] = ArrSaldoFinal[i][9];
          array_aux[2] = 0.0;
          array_aux[3] = 0.0;
          array_aux[4] = 0.0;
          array_aux[5] = 0.0;
          array_aux[6] = 0.0;
          array_aux[7] = 0.0;
          array_aux[8] = ArrSaldoFinal[i][8];
          array_aux[9] = ArrSaldoFinal[i][9];
          array_aux[10] = ArrSaldoFinal[i][10];
          array_aux[11] = ArrSaldoFinal[i][11];
          array_aux[12] = ArrSaldoFinal[i][12];
          array_aux[13] = ArrSaldoFinal[i][13];
          array_aux[14] = ArrSaldoFinal[i][14];
          array_aux[15] = ArrSaldoFinal[i][15];
          array_aux[16] = '';

          array_cuentas.push(array_aux);
          ArrSaldoFinal.splice(i, 0, array_aux);
        }
      }

      //calcular montos de cuentas
      for (var i = 0; i < array_cuentas.length; i++) {
        for (var j = 0; j < ArrSaldoFinal.length; j++) {
          if (array_cuentas[i][0] == ArrSaldoFinal[j][8]) {
            array_cuentas[i][2] += Number(ArrSaldoFinal[j][2]);
            array_cuentas[i][3] += Number(ArrSaldoFinal[j][3]);
            array_cuentas[i][4] += Number(ArrSaldoFinal[j][4]);
            array_cuentas[i][5] += Number(ArrSaldoFinal[j][5]);
            array_cuentas[i][6] += Number(ArrSaldoFinal[j][6]);
            array_cuentas[i][7] += Number(ArrSaldoFinal[j][7]);
          }
        }
      }

      //reemplazar array vacio en el ArrSaldoFinal
      for (var i = 0; i < array_cuentas.length; i++) {
        for (var j = 0; j < ArrSaldoFinal.length; j++) {
          if (array_cuentas[i][0] == ArrSaldoFinal[j][0]) {
            ArrSaldoFinal[j] = array_cuentas[i];
          }
        }
      }

      ArrSaldoFinal = AgregarArregloCuatroDigitosPuc8d(ArrSaldoFinal);

      return ArrSaldoFinal;
    }

    function AgregarArregloCuatroDigitosPuc8d(ArrSaldoFinal) {
      for (var y = 0; y < ArrSaldoFinal.length; y++) {
        /*if (ArrSaldoFinal[y][1] == '' || ArrSaldoFinal[y][1] == null) {
          ArrSaldoFinal[y][1] = 0.0;
        }*/

        if (ArrSaldoFinal[y][2] == '' || ArrSaldoFinal[y][2] == null) {
          ArrSaldoFinal[y][2] = 0.0;
        }

        if (ArrSaldoFinal[y][3] == '' || ArrSaldoFinal[y][3] == null) {
          ArrSaldoFinal[y][3] = 0.0;
        }

        if (ArrSaldoFinal[y][4] == '' || ArrSaldoFinal[y][4] == null) {
          ArrSaldoFinal[y][4] = 0.0;
        }

        if (ArrSaldoFinal[y][5] == '' || ArrSaldoFinal[y][5] == null) {
          ArrSaldoFinal[y][5] = 0.0;
        }

        if (ArrSaldoFinal[y][6] == '' || ArrSaldoFinal[y][6] == null) {
          ArrSaldoFinal[y][6] = 0.0;
        }
      }
      /*
       * ARRAY FINAL
       * 0.  cuenta 8
       * 1.  denominacion 8
       * 2.  debitos antes
       * 3.  creditos antes
       * 4.  debitos actual
       * 5.  creditos actual
       * 6.  nuevo saldo debitos
       * 7.  nuevo saldo creditos
       * 8.  cuenta 6 digitos
       * 9.  denominacion 6 digitos
       * 10.  cuenta 4 digitos
       * 11.  denominacion 4 digitos
       * 12. cuenta 2 digitos
       * 13. denominacion 2 digitos
       * 14. cuenta 1 digito
       * 15. denominacion 1 digito
       * 16. entity
       */

      var cuenta_aux = ArrSaldoFinal[0][10];

      var array_4_digitos = new Array();

      array_4_digitos[0] = cuenta_aux;
      array_4_digitos[1] = ArrSaldoFinal[0][11];
      array_4_digitos[2] = 0.0;
      array_4_digitos[3] = 0.0;
      array_4_digitos[4] = 0.0;
      array_4_digitos[5] = 0.0;
      array_4_digitos[6] = 0.0;
      array_4_digitos[7] = 0.0;
      array_4_digitos[8] = ArrSaldoFinal[0][8];
      array_4_digitos[9] = ArrSaldoFinal[0][9];
      array_4_digitos[10] = ArrSaldoFinal[0][10];
      array_4_digitos[11] = ArrSaldoFinal[0][11];
      array_4_digitos[12] = ArrSaldoFinal[0][12];
      array_4_digitos[13] = ArrSaldoFinal[0][13];
      array_4_digitos[14] = ArrSaldoFinal[0][14];
      array_4_digitos[15] = ArrSaldoFinal[0][15];
      array_4_digitos[16] = '';

      //Agregar al  del array
      ArrSaldoFinal.splice(0, 0, array_4_digitos);

      var array_cuentas = new Array();

      array_cuentas[0] = array_4_digitos;

      var cont = 1;
      for (var i = 0; i < ArrSaldoFinal.length; i++) {
        if (ArrSaldoFinal[i][10] != cuenta_aux) {
          cuenta_aux = ArrSaldoFinal[i][10];
          var array_aux = new Array();

          array_aux[0] = cuenta_aux;
          array_aux[1] = ArrSaldoFinal[i][11];
          array_aux[2] = 0.0;
          array_aux[3] = 0.0;
          array_aux[4] = 0.0;
          array_aux[5] = 0.0;
          array_aux[6] = 0.0;
          array_aux[7] = 0.0;
          array_aux[8] = ArrSaldoFinal[i][8];
          array_aux[9] = ArrSaldoFinal[i][9];
          array_aux[10] = ArrSaldoFinal[i][10];
          array_aux[11] = ArrSaldoFinal[i][11];
          array_aux[12] = ArrSaldoFinal[i][12];
          array_aux[13] = ArrSaldoFinal[i][13];
          array_aux[14] = ArrSaldoFinal[i][14];
          array_aux[15] = ArrSaldoFinal[i][15];
          array_aux[16] = '';

          array_cuentas[cont] = array_aux;
          cont++;
          ArrSaldoFinal.splice(i, 0, array_aux);
        }
      }

      //calcular montos de cuentas
      for (var i = 0; i < array_cuentas.length; i++) {
        for (var j = 0; j < ArrSaldoFinal.length; j++) {
          if (array_cuentas[i][0] == ArrSaldoFinal[j][10] && ArrSaldoFinal[j][0].length == 8) {
            array_cuentas[i][2] += Number(ArrSaldoFinal[j][2]);
            array_cuentas[i][3] += Number(ArrSaldoFinal[j][3]);
            array_cuentas[i][4] += Number(ArrSaldoFinal[j][4]);
            array_cuentas[i][5] += Number(ArrSaldoFinal[j][5]);
            array_cuentas[i][6] += Number(ArrSaldoFinal[j][6]);
            array_cuentas[i][7] += Number(ArrSaldoFinal[j][7]);
          }
        }
      }

      //reemplazar array vacio en el ArrSaldoFinal
      for (var i = 0; i < array_cuentas.length; i++) {
        for (var j = 0; j < ArrSaldoFinal.length; j++) {
          if (array_cuentas[i][0] == ArrSaldoFinal[j][0]) {
            ArrSaldoFinal[j] = array_cuentas[i];
          }
        }
      }

      ArrSaldoFinal = AgregarArregloDosDigitosPuc8d(ArrSaldoFinal);

      return ArrSaldoFinal;
    }

    function AgregarArregloCuatroDigitos(ArrSaldoFinal) {
      for (var y = 0; y < ArrSaldoFinal.length; y++) {
        if (ArrSaldoFinal[y][1] == '' || ArrSaldoFinal[y][1] == null) {
          ArrSaldoFinal[y][1] = 0.0;
        }

        if (ArrSaldoFinal[y][2] == '' || ArrSaldoFinal[y][2] == null) {
          ArrSaldoFinal[y][2] = 0.0;
        }

        if (ArrSaldoFinal[y][3] == '' || ArrSaldoFinal[y][3] == null) {
          ArrSaldoFinal[y][3] = 0.0;
        }

        if (ArrSaldoFinal[y][4] == '' || ArrSaldoFinal[y][4] == null) {
          ArrSaldoFinal[y][4] = 0.0;
        }

        if (ArrSaldoFinal[y][5] == '' || ArrSaldoFinal[y][5] == null) {
          ArrSaldoFinal[y][5] = 0.0;
        }

        if (ArrSaldoFinal[y][6] == '' || ArrSaldoFinal[y][6] == null) {
          ArrSaldoFinal[y][6] = 0.0;
        }
      }
      /*
       * ARRAY FINAL
       * 0.  cuenta 6
       * 1.  denominacion 6
       * 2.  debitos antes
       * 3.  creditos antes
       * 4.  debitos actual
       * 5.  creditos actual
       * 6.  nuevo saldo debitos
       * 7.  nuevo saldo creditos
       * 8.  cuenta 4 digitos
       * 9.  denominacion 4 digitos
       * 10. cuenta 2 digitos
       * 11. denominacion 2 digitos
       * 12. cuenta 1 digito
       * 13. denominacion 1 digito
       * 14. entity
       */

      var cuenta_aux = ArrSaldoFinal[0][8];

      var array_4_digitos = new Array();

      array_4_digitos[0] = cuenta_aux;
      array_4_digitos[1] = ArrSaldoFinal[0][9];
      array_4_digitos[2] = 0.0;
      array_4_digitos[3] = 0.0;
      array_4_digitos[4] = 0.0;
      array_4_digitos[5] = 0.0;
      array_4_digitos[6] = 0.0;
      array_4_digitos[7] = 0.0;
      array_4_digitos[8] = ArrSaldoFinal[0][8];
      array_4_digitos[9] = ArrSaldoFinal[0][9];
      array_4_digitos[10] = ArrSaldoFinal[0][10];
      array_4_digitos[11] = ArrSaldoFinal[0][11];
      array_4_digitos[12] = ArrSaldoFinal[0][12];
      array_4_digitos[13] = ArrSaldoFinal[0][13];
      array_4_digitos[14] = '';

      //Agregar al  del array
      ArrSaldoFinal.splice(0, 0, array_4_digitos);

      var array_cuentas = new Array();

      array_cuentas[0] = array_4_digitos;

      var cont = 1;
      for (var i = 0; i < ArrSaldoFinal.length; i++) {
        if (ArrSaldoFinal[i][8] != cuenta_aux) {
          cuenta_aux = ArrSaldoFinal[i][8];
          var array_aux = new Array();

          array_aux[0] = cuenta_aux;
          array_aux[1] = ArrSaldoFinal[i][9];
          array_aux[2] = 0.0;
          array_aux[3] = 0.0;
          array_aux[4] = 0.0;
          array_aux[5] = 0.0;
          array_aux[6] = 0.0;
          array_aux[7] = 0.0;
          array_aux[8] = ArrSaldoFinal[i][8];
          array_aux[9] = ArrSaldoFinal[i][9];
          array_aux[10] = ArrSaldoFinal[i][10];
          array_aux[11] = ArrSaldoFinal[i][11];
          array_aux[12] = ArrSaldoFinal[i][12];
          array_aux[13] = ArrSaldoFinal[i][13];
          array_aux[14] = '';

          array_cuentas[cont] = array_aux;
          cont++;
          ArrSaldoFinal.splice(i, 0, array_aux);
        }
      }

      //calcular montos de cuentas
      for (var i = 0; i < array_cuentas.length; i++) {
        for (var j = 0; j < ArrSaldoFinal.length; j++) {
          if (array_cuentas[i][0] == ArrSaldoFinal[j][8]) {
            array_cuentas[i][2] += Number(ArrSaldoFinal[j][2]);
            array_cuentas[i][3] += Number(ArrSaldoFinal[j][3]);
            array_cuentas[i][4] += Number(ArrSaldoFinal[j][4]);
            array_cuentas[i][5] += Number(ArrSaldoFinal[j][5]);
            array_cuentas[i][6] += Number(ArrSaldoFinal[j][6]);
            array_cuentas[i][7] += Number(ArrSaldoFinal[j][7]);
          }
        }
      }

      //reemplazar array vacio en el ArrSaldoFinal
      for (var i = 0; i < array_cuentas.length; i++) {
        for (var j = 0; j < ArrSaldoFinal.length; j++) {
          if (array_cuentas[i][0] == ArrSaldoFinal[j][0]) {
            ArrSaldoFinal[j] = array_cuentas[i];
          }
        }
      }

      ArrSaldoFinal = AgregarArregloDosDigitos(ArrSaldoFinal);

      return ArrSaldoFinal;
    }

    function AgregarArregloDosDigitosPuc8d(ArrSaldoFinal) {
      /*
       * ARRAY FINAL
       * 0.  cuenta 8
       * 1.  denominacion 8
       * 2.  debitos antes
       * 3.  creditos antes
       * 4.  debitos actual
       * 5.  creditos actual
       * 6.  nuevo saldo debitos
       * 7.  nuevo saldo creditos
       * 8.  cuenta 6 digitos
       * 9.  denominacion 6 digitos
       * 10.  cuenta 4 digitos
       * 11.  denominacion 4 digitos
       * 12. cuenta 2 digitos
       * 13. denominacion 2 digitos
       * 14. cuenta 1 digito
       * 15. denominacion 1 digito
       * 16. entity
       */

      var grupo_aux = ArrSaldoFinal[0][12];

      var array_aux_uno = new Array();

      array_aux_uno[0] = grupo_aux;
      array_aux_uno[1] = ArrSaldoFinal[0][13];
      array_aux_uno[2] = 0;
      array_aux_uno[3] = 0;
      array_aux_uno[4] = 0;
      array_aux_uno[5] = 0;
      array_aux_uno[6] = 0;
      array_aux_uno[7] = 0;
      array_aux_uno[8] = ArrSaldoFinal[0][8];
      array_aux_uno[9] = ArrSaldoFinal[0][9];
      array_aux_uno[10] = ArrSaldoFinal[0][10];
      array_aux_uno[11] = ArrSaldoFinal[0][11];
      array_aux_uno[12] = ArrSaldoFinal[0][12];
      array_aux_uno[13] = ArrSaldoFinal[0][13];
      array_aux_uno[14] = ArrSaldoFinal[0][14];
      array_aux_uno[15] = ArrSaldoFinal[0][15];
      array_aux_uno[16] = '';

      ArrSaldoFinal.splice(0, 0, array_aux_uno);

      var array_cuentas = new Array();
      array_cuentas[0] = array_aux_uno;
      //quiebre de grupo
      for (var i = 0; i < ArrSaldoFinal.length; i++) {
        if (grupo_aux != ArrSaldoFinal[i][12]) {
          grupo_aux = ArrSaldoFinal[i][12];
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
          array_aux[1] = ArrSaldoFinal[i][13];
          array_aux[2] = 0.0;
          array_aux[3] = 0.0;
          array_aux[4] = 0.0;
          array_aux[5] = 0.0;
          array_aux[6] = 0.0;
          array_aux[7] = 0.0;
          array_aux[8] = ArrSaldoFinal[i][8];
          array_aux[9] = ArrSaldoFinal[i][9];
          array_aux[10] = ArrSaldoFinal[i][10];
          array_aux[11] = ArrSaldoFinal[i][11];
          array_aux[12] = ArrSaldoFinal[i][12];
          array_aux[13] = ArrSaldoFinal[i][13];
          array_aux[14] = ArrSaldoFinal[i][14];
          array_aux[15] = ArrSaldoFinal[i][15];
          array_aux[16] = '';

          array_cuentas.push(array_aux);
          ArrSaldoFinal.splice(i, 0, array_aux);
        }
      }

      //calcular montos de cuentas
      for (var i = 0; i < array_cuentas.length; i++) {
        for (var j = 0; j < ArrSaldoFinal.length; j++) {
          if (array_cuentas[i][0] == ArrSaldoFinal[j][12] && ArrSaldoFinal[j][0].length == 8) {
            array_cuentas[i][2] += Number(ArrSaldoFinal[j][2]);
            array_cuentas[i][3] += Number(ArrSaldoFinal[j][3]);
            array_cuentas[i][4] += Number(ArrSaldoFinal[j][4]);
            array_cuentas[i][5] += Number(ArrSaldoFinal[j][5]);
            array_cuentas[i][6] += Number(ArrSaldoFinal[j][6]);
            array_cuentas[i][7] += Number(ArrSaldoFinal[j][7]);
          }
        }
      }

      //reemplazar array vacio del ArrSaldoFinal
      for (var i = 0; i < array_cuentas.length; i++) {
        for (var j = 0; j < ArrSaldoFinal.length; j++) {
          if (array_cuentas[i][0] == ArrSaldoFinal[j][0]) {
            ArrSaldoFinal[j] = array_cuentas[i];
          }
        }
      }

      ArrSaldoFinal = AgregarArregloUnDigitoPuc8d(ArrSaldoFinal);

      return ArrSaldoFinal;
    }

    function AgregarArregloDosDigitos(ArrSaldoFinal) {
      /*
       * ARRAY FINAL
       * 0.  cuenta 6
       * 1.  denominacion 6
       * 2.  debitos antes
       * 3.  creditos antes
       * 4.  debitos actual
       * 5.  creditos actual
       * 6.  nuevo saldo debitos
       * 7.  nuevo saldo creditos
       * 8.  cuenta 4 digitos
       * 9.  denominacion 4 digitos
       * 10. cuenta 2 digitos
       * 11. denominacion 2 digitos
       * 12. cuenta 1 digito
       * 13. denominacion 1 digito
       * 14. entity
       */

      var grupo_aux = ArrSaldoFinal[0][10];

      var array_aux_uno = new Array();

      array_aux_uno[0] = grupo_aux;
      array_aux_uno[1] = ArrSaldoFinal[0][11];
      array_aux_uno[2] = 0;
      array_aux_uno[3] = 0;
      array_aux_uno[4] = 0;
      array_aux_uno[5] = 0;
      array_aux_uno[6] = 0;
      array_aux_uno[7] = 0;
      array_aux_uno[8] = ArrSaldoFinal[0][8];
      array_aux_uno[9] = ArrSaldoFinal[0][9];
      array_aux_uno[10] = ArrSaldoFinal[0][10];
      array_aux_uno[11] = ArrSaldoFinal[0][11];
      array_aux_uno[12] = ArrSaldoFinal[0][12];
      array_aux_uno[13] = ArrSaldoFinal[0][13];
      array_aux_uno[14] = '';

      ArrSaldoFinal.splice(0, 0, array_aux_uno);

      var array_cuentas = new Array();

      array_cuentas[0] = array_aux_uno;

      var cont = 1;

      //quiebre de grupo
      for (var i = 0; i < ArrSaldoFinal.length; i++) {
        if (grupo_aux != ArrSaldoFinal[i][10]) {
          grupo_aux = ArrSaldoFinal[i][10];
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
          array_aux[1] = ArrSaldoFinal[i][11];
          array_aux[2] = 0.0;
          array_aux[3] = 0.0;
          array_aux[4] = 0.0;
          array_aux[5] = 0.0;
          array_aux[6] = 0.0;
          array_aux[7] = 0.0;
          array_aux[8] = ArrSaldoFinal[i][8];
          array_aux[9] = ArrSaldoFinal[i][9];
          array_aux[10] = ArrSaldoFinal[i][10];
          array_aux[11] = ArrSaldoFinal[i][11];
          array_aux[12] = ArrSaldoFinal[i][12];
          array_aux[13] = ArrSaldoFinal[i][13];
          array_aux[14] = '';

          array_cuentas[cont] = array_aux;
          cont++;
          ArrSaldoFinal.splice(i, 0, array_aux);
        }
      }

      //calcular montos de cuentas
      for (var i = 0; i < array_cuentas.length; i++) {
        for (var j = 0; j < ArrSaldoFinal.length; j++) {
          if (array_cuentas[i][0] == ArrSaldoFinal[j][10] && ArrSaldoFinal[j][0].length == 6) {
            array_cuentas[i][2] += Number(ArrSaldoFinal[j][2]);
            array_cuentas[i][3] += Number(ArrSaldoFinal[j][3]);
            array_cuentas[i][4] += Number(ArrSaldoFinal[j][4]);
            array_cuentas[i][5] += Number(ArrSaldoFinal[j][5]);
            array_cuentas[i][6] += Number(ArrSaldoFinal[j][6]);
            array_cuentas[i][7] += Number(ArrSaldoFinal[j][7]);
          }
        }
      }

      //reemplazar array vacio del ArrSaldoFinal
      for (var i = 0; i < array_cuentas.length; i++) {
        for (var j = 0; j < ArrSaldoFinal.length; j++) {
          if (array_cuentas[i][0] == ArrSaldoFinal[j][0]) {
            ArrSaldoFinal[j] = array_cuentas[i];
          }
        }
      }

      ArrSaldoFinal = AgregarArregloUnDigito(ArrSaldoFinal);

      return ArrSaldoFinal;
    }

    function AgregarArregloUnDigitoPuc8d(ArrSaldoFinal) {
      /*
       * ARRAY FINAL
       * 0.  cuenta 8
       * 1.  denominacion 8
       * 2.  debitos antes
       * 3.  creditos antes
       * 4.  debitos actual
       * 5.  creditos actual
       * 6.  nuevo saldo debitos
       * 7.  nuevo saldo creditos
       * 8.  cuenta 6 digitos
       * 9.  denominacion 6 digitos
       * 10.  cuenta 4 digitos
       * 11.  denominacion 4 digitos
       * 12. cuenta 2 digitos
       * 13. denominacion 2 digitos
       * 14. cuenta 1 digito
       * 15. denominacion 1 digito
       * 16. entity
       */
      var clase_aux = ArrSaldoFinal[0][14];

      var array_aux_uno = new Array();
      array_aux_uno[0] = clase_aux;
      array_aux_uno[1] = ArrSaldoFinal[0][15];
      array_aux_uno[2] = 0.0;
      array_aux_uno[3] = 0.0;
      array_aux_uno[4] = 0.0;
      array_aux_uno[5] = 0.0;
      array_aux_uno[6] = 0.0;
      array_aux_uno[7] = 0.0;
      array_aux_uno[8] = ArrSaldoFinal[0][8];
      array_aux_uno[9] = ArrSaldoFinal[0][9];
      array_aux_uno[10] = ArrSaldoFinal[0][10];
      array_aux_uno[11] = ArrSaldoFinal[0][11];
      array_aux_uno[12] = ArrSaldoFinal[0][12];
      array_aux_uno[13] = ArrSaldoFinal[0][13];
      array_aux_uno[14] = ArrSaldoFinal[0][14];
      array_aux_uno[15] = ArrSaldoFinal[0][15];
      array_aux_uno[16] = '';

      ArrSaldoFinal.splice(0, 0, array_aux_uno);

      var array_cuentas = new Array();
      array_cuentas[0] = array_aux_uno;
      //quiebre de grupo
      for (var i = 0; i < ArrSaldoFinal.length; i++) {
        if (ArrSaldoFinal[i][14] != clase_aux) {
          clase_aux = ArrSaldoFinal[i][14];
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
          array_aux[1] = ArrSaldoFinal[i][15];
          array_aux[2] = 0.0;
          array_aux[3] = 0.0;
          array_aux[4] = 0.0;
          array_aux[5] = 0.0;
          array_aux[6] = 0.0;
          array_aux[7] = 0.0;
          array_aux[8] = ArrSaldoFinal[i][8];
          array_aux[9] = ArrSaldoFinal[i][9];
          array_aux[10] = ArrSaldoFinal[i][10];
          array_aux[11] = ArrSaldoFinal[i][11];
          array_aux[12] = ArrSaldoFinal[i][12];
          array_aux[13] = ArrSaldoFinal[i][13];
          array_aux[14] = ArrSaldoFinal[i][14];
          array_aux[15] = ArrSaldoFinal[i][15];
          array_aux[16] = '';

          array_cuentas.push(array_aux);
          ArrSaldoFinal.splice(i, 0, array_aux);
        }
      }
      //calcular montos de cuentas
      for (var i = 0; i < array_cuentas.length; i++) {

        for (var j = 0; j < ArrSaldoFinal.length; j++) {
          if (array_cuentas[i][0] == ArrSaldoFinal[j][14] && ArrSaldoFinal[j][0] != null && ArrSaldoFinal[j][0].length == 8) {
            array_cuentas[i][2] += Number(ArrSaldoFinal[j][2]);
            array_cuentas[i][3] += Number(ArrSaldoFinal[j][3]);
            array_cuentas[i][4] += Number(ArrSaldoFinal[j][4]);
            array_cuentas[i][5] += Number(ArrSaldoFinal[j][5]);
            array_cuentas[i][6] += Number(ArrSaldoFinal[j][6]);
            array_cuentas[i][7] += Number(ArrSaldoFinal[j][7]);
          }
        }
      }

      //reemplazar array vacio del ArrSaldoFinal
      for (var i = 0; i < array_cuentas.length; i++) {
        for (var j = 0; j < ArrSaldoFinal.length; j++) {
          if (array_cuentas[i][0] == ArrSaldoFinal[j][0]) {
            ArrSaldoFinal[j] = array_cuentas[i];
          }
        }
      }

      return ArrSaldoFinal;
    }

    function AgregarArregloUnDigito(ArrSaldoFinal) {
      /*
       * ARRAY FINAL
       * 0.  cuenta 6
       * 1.  denominacion 6
       * 2.  debitos antes
       * 3.  creditos antes
       * 4.  debitos actual
       * 5.  creditos actual
       * 6.  nuevo saldo debitos
       * 7.  nuevo saldo creditos
       * 8.  cuenta 4 digitos
       * 9.  denominacion 4 digitos
       * 10. cuenta 2 digitos
       * 11. denominacion 2 digitos
       * 12. cuenta 1 digito
       * 13. denominacion 1 digito
       * 14. entity
       */
      var clase_aux = ArrSaldoFinal[0][12];

      var array_aux_uno = new Array();
      array_aux_uno[0] = clase_aux;
      array_aux_uno[1] = ArrSaldoFinal[0][13];
      array_aux_uno[2] = 0.0;
      array_aux_uno[3] = 0.0;
      array_aux_uno[4] = 0.0;
      array_aux_uno[5] = 0.0;
      array_aux_uno[6] = 0.0;
      array_aux_uno[7] = 0.0;
      array_aux_uno[8] = ArrSaldoFinal[0][8];
      array_aux_uno[9] = ArrSaldoFinal[0][9];
      array_aux_uno[10] = ArrSaldoFinal[0][10];
      array_aux_uno[11] = ArrSaldoFinal[0][11];
      array_aux_uno[12] = ArrSaldoFinal[0][12];
      array_aux_uno[13] = ArrSaldoFinal[0][13];
      array_aux_uno[14] = '';

      ArrSaldoFinal.splice(0, 0, array_aux_uno);

      var array_cuentas = new Array();

      array_cuentas[0] = array_aux_uno;

      var cont = 1;

      //quiebre de grupo
      for (var i = 0; i < ArrSaldoFinal.length; i++) {
        if (ArrSaldoFinal[i][12] != clase_aux) {
          clase_aux = ArrSaldoFinal[i][12];
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
          array_aux[1] = ArrSaldoFinal[i][13];
          array_aux[2] = 0.0;
          array_aux[3] = 0.0;
          array_aux[4] = 0.0;
          array_aux[5] = 0.0;
          array_aux[6] = 0.0;
          array_aux[7] = 0.0;
          array_aux[8] = ArrSaldoFinal[i][8];
          array_aux[9] = ArrSaldoFinal[i][9];
          array_aux[10] = ArrSaldoFinal[i][10];
          array_aux[11] = ArrSaldoFinal[i][11];
          array_aux[12] = ArrSaldoFinal[i][12];
          array_aux[13] = ArrSaldoFinal[i][13];
          array_aux[14] = '';

          array_cuentas[cont] = array_aux;
          cont++;
          ArrSaldoFinal.splice(i, 0, array_aux);
        }
      }
      //calcular montos de cuentas
      for (var i = 0; i < array_cuentas.length; i++) {

        for (var j = 0; j < ArrSaldoFinal.length; j++) {
          if (array_cuentas[i][0] == ArrSaldoFinal[j][12] && ArrSaldoFinal[j][0] != null && ArrSaldoFinal[j][0].length == 6) {
            array_cuentas[i][2] += Number(ArrSaldoFinal[j][2]);
            array_cuentas[i][3] += Number(ArrSaldoFinal[j][3]);
            array_cuentas[i][4] += Number(ArrSaldoFinal[j][4]);
            array_cuentas[i][5] += Number(ArrSaldoFinal[j][5]);
            array_cuentas[i][6] += Number(ArrSaldoFinal[j][6]);
            array_cuentas[i][7] += Number(ArrSaldoFinal[j][7]);
          }
        }
      }

      //reemplazar array vacio del ArrSaldoFinal
      for (var i = 0; i < array_cuentas.length; i++) {
        for (var j = 0; j < ArrSaldoFinal.length; j++) {
          if (array_cuentas[i][0] == ArrSaldoFinal[j][0]) {
            ArrSaldoFinal[j] = array_cuentas[i];
          }
        }
      }

      return ArrSaldoFinal;
    }

    function CambiarDataCuenta(ArrData) {
      /* ArrData formato
       * 0. Account ID
       * 1. Debit SA
       * 2. Credit SA
       * 3. Debit Mov
       * 4. Credit Mob
       * 5. Debit Final
       * 6. Credit Final
       * 7. Entity
       */

      /**
       * ArrAccounts formato
       */
      //0. Internal Id
      //1. number
      //2. puc 1 id
      //3. puc 1 des
      //4. puc 2 id
      //5. puc 2 des
      //6. puc 4 id
      //7. puc 4 des
      //8. puc 6 id
      //9. puc 6 des
      //10. puc 8 id
      //11. puc 8 des
      var ArrReturn = new Array();

      for (var i = 0; i < ArrData.length; i++) {
        for (var j = 0; j < ArrAccounts.length; j++) {
          if (ArrData[i][0] == ArrAccounts[j][0]) {
            var ArrAux = new Array();
            ArrAux[0] = ArrAccounts[j][8];
            ArrAux[1] = ArrAccounts[j][9];
            ArrAux[2] = ArrData[i][1];
            ArrAux[3] = ArrData[i][2];
            ArrAux[4] = ArrData[i][3];
            ArrAux[5] = ArrData[i][4];
            ArrAux[6] = ArrData[i][5];
            ArrAux[7] = ArrData[i][6];
            ArrAux[8] = ArrAccounts[j][6];
            ArrAux[9] = ArrAccounts[j][7];
            ArrAux[10] = ArrAccounts[j][4];
            ArrAux[11] = ArrAccounts[j][5];
            ArrAux[12] = ArrAccounts[j][2];
            ArrAux[13] = ArrAccounts[j][3];
            ArrAux[14] = ArrData[i][7];

            ArrReturn.push(ArrAux);
            //ArrAccounts.splice(j, 1);
            break;
          }
        }
      }

      return ArrReturn;
    }

    function CambiarDataCuentaPuc8d(ArrData) {
      /* ArrData formato
       * 0. Account ID
       * 1. Debit SA
       * 2. Credit SA
       * 3. Debit Mov
       * 4. Credit Mob
       * 5. Debit Final
       * 6. Credit Final
       * 7. Entity
       */

      /**
       * ArrAccounts formato
       */
      //0. Internal Id
      //1. number
      //2. puc 1 id
      //3. puc 1 des
      //4. puc 2 id
      //5. puc 2 des
      //6. puc 4 id
      //7. puc 4 des
      //8. puc 6 id
      //9. puc 6 des
      //10. puc 8 id
      //11. puc 8 des
      var ArrReturn = new Array();

      for (var i = 0; i < ArrData.length; i++) {
        for (var j = 0; j < ArrAccounts.length; j++) {
          if (ArrData[i][0] == ArrAccounts[j][0]) {
            var ArrAux = new Array();
            ArrAux[0] = ArrAccounts[j][10];
            ArrAux[1] = ArrAccounts[j][11];
            ArrAux[2] = ArrData[i][1];
            ArrAux[3] = ArrData[i][2];
            ArrAux[4] = ArrData[i][3];
            ArrAux[5] = ArrData[i][4];
            ArrAux[6] = ArrData[i][5];
            ArrAux[7] = ArrData[i][6];
            ArrAux[8] = ArrAccounts[j][8];
            ArrAux[9] = ArrAccounts[j][9];
            ArrAux[10] = ArrAccounts[j][6];
            ArrAux[11] = ArrAccounts[j][7];
            ArrAux[12] = ArrAccounts[j][4];
            ArrAux[13] = ArrAccounts[j][5];
            ArrAux[14] = ArrAccounts[j][2];
            ArrAux[15] = ArrAccounts[j][3];
            ArrAux[16] = ArrData[i][7];

            ArrReturn.push(ArrAux);
            break;
          }
        }
      }

      return ArrReturn;
    }

    function ConvertToArray(strFile) {
      var rows = strFile.split('\r\n');
      var ArrReturn = new Array();
      var cont = 0;

      for (var i = 0; i < rows.length - 1; i++) {
        var columns = rows[i].split('|');
        var arr = new Array();

        for (var j = 0; j < columns.length; j++) {
          arr[j] = columns[j];
        }

        ArrReturn[cont] = arr;
        cont++;
      }

      return ArrReturn;
    }

    function ObtenerFile() {
      var transactionFile = fileModulo.load({
        id: paramFileID
      });

      return transactionFile.getContents();
    }

    function ObtenerParametros() {
    
      params = JSON.parse(
        objContext.getParameter("custscript_lmry_co_terc_schdl")
      );
      log.debug('params', params);
      paramFileID = params.fileId;
      paramRecordID = params.recordId;
      paramEntityID = params.entityId;
      paramMultibook = params.multibookId;
      paramSubsidy = params.subsidiaryId;
      paramPeriod = params.period.initial;
      paramLastPuc = params.lastPucIndex;
      paramPeriodFin = params.period.final;
      paramAdjustment = params.period.isAdjustment;
      paramOpenBalance = params.isOpenBalance;
      paramPuc8D = params.isCopuc8;
      paramStep = params.step || null;

      if (paramLastPuc == null) {
        paramLastPuc = 1;
      }

      if (paramStep == null) {
        paramStep = 0;
      }

      ObtenerDatosSubsidiaria();

      var period_temp = search.lookupFields({
        type: search.Type.ACCOUNTING_PERIOD,
        id: paramPeriod,
        columns: ['periodname', 'startdate', 'enddate']
      });

      periodenddate = period_temp.enddate;
      periodstartdate = period_temp.startdate;
      periodname = period_temp.periodname;
      periodnameLog = periodname;

      if (paramPeriodFin != null && paramPeriodFin != '') {
        var period_temp_final = search.lookupFields({
          type: search.Type.ACCOUNTING_PERIOD,
          id: paramPeriodFin,
          columns: ['periodname']
        });

        periodnamefinal = period_temp_final.periodname;
        periodnamefinalLog = periodnamefinal;
      }

      if (featAccountingSpecial || featAccountingSpecial == 'T') {
        var jsonPeriodIni = getSpecialPeriod(paramPeriod);
        log.debug('jsonPeriodIni', jsonPeriodIni);
        periodenddate = jsonPeriodIni.periodenddate;
        periodstartdate = jsonPeriodIni.periodstartdate;
        periodname = jsonPeriodIni.periodname;

        if (paramPeriodFin != null && paramPeriodFin != '') {
          var jsonPeriodFin = getSpecialPeriod(paramPeriodFin);
          periodnamefinal = jsonPeriodFin.periodname;
        }
      }

      if (feamultibook || feamultibook == 'T') {
        var multibookName_temp = search.lookupFields({
          type: search.Type.ACCOUNTING_BOOK,
          id: paramMultibook,
          columns: ['name']
        });

        multibookName = multibookName_temp.name;
      }

    }

    function getSpecialPeriod(periodID) {
      var jsonData = {
        periodname: '',
        periodenddate: '',
        periodstartdate: ''
      };

      var searchSpecialPeriod = search.create({
        type: "customrecord_lmry_special_accountperiod",
        filters: [
          ["isinactive", "is", "F"], 'AND',
          ["custrecord_lmry_accounting_period", "is", periodID]
        ],
        columns: [
          search.createColumn({
            name: "custrecord_lmry_calendar",
            label: "0. Latam - Calendar"
          }),
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

      var pagedData = searchSpecialPeriod.runPaged({
        pageSize: 1000
      });

      pagedData.pageRanges.forEach(function(pageRange) {
        page = pagedData.fetch({
          index: pageRange.index
        });

        page.data.forEach(function(result) {
          columns = result.columns;
          if (calendarSubsi != null && calendarSubsi != '') {
            var calendar = result.getValue(columns[0]);
            if (calendar != null && calendar != '') {
              calendar = JSON.parse(calendar);
              if (calendar.id == calendarSubsi) {
                jsonData.periodname = result.getValue(columns[3]);
                jsonData.periodenddate = result.getValue(columns[2]);
                jsonData.periodstartdate = result.getValue(columns[1]);
              }

            } else {
              log.debug('No existe fiscal calendar para periodo special.')
            }
          } else {
            jsonData.periodname = result.getValue(columns[3]);
            jsonData.periodenddate = result.getValue(columns[2]);
            jsonData.periodstartdate = result.getValue(columns[1]);
          }

        })
      });

      return jsonData;
    }

    function ObtenerCuentas() {

      var intDMinReg = 0;
      var intDMaxReg = 1000;

      var infoTxt = '';
      var DbolStop = false;

      var ArrReturn = new Array();
      var cont = 0;

      var busqueda = search.create({
        type: search.Type.ACCOUNT,
        filters: [
          ['custrecord_lmry_co_puc_d6_id', 'isnotempty', '']
        ],
        columns: ['internalid', 'number', 'custrecord_lmry_co_puc_d1_id', 'custrecord_lmry_co_puc_d1_description', 'custrecord_lmry_co_puc_d2_id', 'custrecord_lmry_co_puc_d2_description', 'custrecord_lmry_co_puc_d4_id', 'custrecord_lmry_co_puc_d4_description', 'custrecord_lmry_co_puc_d6_id', 'custrecord_lmry_co_puc_d6_description', 'custrecord_lmry_co_puc_id', 'custrecord_lmry_co_puc_description']
      });

      if (paramPuc8D) {
        var puc8dFilter = search.createFilter({
          name: 'custrecord_lmry_co_puc_id',
          operator: search.Operator.ISNOTEMPTY
        });
        busqueda.filters.push(puc8dFilter);
      }

      if (featuresubs) {
        var subsidiaryFilter = search.createFilter({
          name: 'subsidiary',
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
            //0. Internal Id
            if (objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '')
              arrAuxiliar[0] = objResult[i].getValue(columns[0]);
            else
              arrAuxiliar[0] = '';
            //1. number
            if (objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '')
              arrAuxiliar[1] = objResult[i].getValue(columns[1]);
            else
              arrAuxiliar[1] = '';
            //2. puc 1 id
            if (objResult[i].getText(columns[2]) != null && objResult[i].getText(columns[2]) != '')
              arrAuxiliar[2] = objResult[i].getText(columns[2]);
            else
              arrAuxiliar[2] = '';
            //3. puc 1 des
            if (objResult[i].getValue(columns[3]) != null && objResult[i].getValue(columns[3]) != '')
              arrAuxiliar[3] = validateSplitLine(objResult[i].getValue(columns[3]));
            else
              arrAuxiliar[3] = '';
            //4. puc 2 id
            if (objResult[i].getText(columns[4]) != null && objResult[i].getText(columns[4]) != '')
              arrAuxiliar[4] = objResult[i].getText(columns[4]);
            else
              arrAuxiliar[4] = '';
            //5. puc 2 des
            if (objResult[i].getValue(columns[5]) != null && objResult[i].getValue(columns[5]) != '')
              arrAuxiliar[5] = validateSplitLine(objResult[i].getValue(columns[5]));
            else
              arrAuxiliar[5] = '';
            //6. puc 4 id
            if (objResult[i].getText(columns[6]) != null && objResult[i].getText(columns[6]) != '')
              arrAuxiliar[6] = objResult[i].getText(columns[6]);
            else
              arrAuxiliar[6] = '';
            //7. puc 4 des
            if (objResult[i].getValue(columns[7]) != null && objResult[i].getValue(columns[7]) != '')
              arrAuxiliar[7] = validateSplitLine(objResult[i].getValue(columns[7]));
            else
              arrAuxiliar[7] = '';
            //8. puc 6 id
            if (objResult[i].getText(columns[8]) != null && objResult[i].getText(columns[8]) != '')
              arrAuxiliar[8] = objResult[i].getText(columns[8]);
            else
              arrAuxiliar[8] = '';
            //9. puc 6 des
            if (objResult[i].getValue(columns[9]) != null && objResult[i].getValue(columns[9]) != '')
              arrAuxiliar[9] = validateSplitLine(objResult[i].getValue(columns[9]));
            else
              arrAuxiliar[9] = '';
            //10. puc 8 id
            if (objResult[i].getText(columns[10]) != null && objResult[i].getText(columns[10]) != '')
              arrAuxiliar[10] = objResult[i].getText(columns[10]);
            else
              arrAuxiliar[10] = '';
            //11. puc 8 des
            if (objResult[i].getValue(columns[11]) != null && objResult[i].getValue(columns[11]) != '')
              arrAuxiliar[11] = validateSplitLine(objResult[i].getValue(columns[11]));
            else
              arrAuxiliar[11] = '';


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
      log.debug('Cuentas: ', ArrReturn);

      return ArrReturn;
    }

    function validateSplitLine(descripcion){
      var regex = /\r\n/gi;
      var resultDescripcion = descripcion.replace(regex, ' ')
      return resultDescripcion;
    }

    function savefile(Final_string) {
      var FolderId = objContext.getParameter({
        name: 'custscript_lmry_file_cabinet_rg_co'
      });

      // Almacena en la carpeta de Archivos Generados
      if (FolderId != '' && FolderId != null) {
        if (feamultibook || feamultibook == 'T') {
          var Final_NameFile = 'Reporte_Balance_Comprobacion_Terceros_' + paramPeriod + '_' + paramMultibook + '_' + paramLastPuc + '.xls';
        } else {
          var Final_NameFile = 'Reporte_Balance_Comprobacion_Terceros_' + paramPeriod + '_' + paramLastPuc + '.xls';
        }
        // Crea el archivo.xls
        var file = fileModulo.create({
          name: Final_NameFile,
          fileType: fileModulo.Type.EXCEL,
          contents: Final_string,
          folder: FolderId
        });

        var idfile = file.save(); // Termina de grabar el archivo
        var idfile2 = fileModulo.load({
          id: idfile
        }); // Trae URL de archivo generado

        // Obtenemo de las prefencias generales el URL de Netsuite (Produccion o Sandbox)
        var getURL = objContext.getParameter({
          name: 'custscript_lmry_netsuite_location'
        });
        var urlfile = '';

        if (getURL != '' && getURL != '') {
          urlfile += 'https://' + getURL;
        }

        urlfile += idfile2.url;

        var usuarioTemp = runtime.getCurrentUser();
        var usuario = usuarioTemp.name;

        if (paramRecordID != null && paramRecordID != '') {
          var record = recordModulo.load({
            type: 'customrecord_lmry_co_rpt_generator_log',
            id: paramRecordID
          });
        } else {
          var record = recordModulo.create({
            type: 'customrecord_lmry_co_rpt_generator_log'
          });
        }

        //Nombre de Archivo
        record.setValue({
          fieldId: 'custrecord_lmry_co_rg_name',
          value: Final_NameFile
        });
        //Url de Archivo
        record.setValue({
          fieldId: 'custrecord_lmry_co_rg_url_file',
          value: urlfile
        });
        //Nombre de Reporte
        record.setValue({
          fieldId: 'custrecord_lmry_co_rg_transaction',
          value: namereport
        });
        //Nombre de Subsidiaria
        record.setValue({
          fieldId: 'custrecord_lmry_co_rg_subsidiary',
          value: companyname
        });
        //Periodo
        if (periodnamefinalLog != null && periodnamefinalLog != '') {
          record.setValue({
            fieldId: 'custrecord_lmry_co_rg_postingperiod',
            value: periodnameLog + ' - ' + periodnamefinalLog
          });
        } else {
          record.setValue({
            fieldId: 'custrecord_lmry_co_rg_postingperiod',
            value: periodnameLog
          });
        }
        //Multibook
        if (feamultibook || feamultibook == 'T') {
          record.setValue({
            fieldId: 'custrecord_lmry_co_rg_multibook',
            value: multibookName
          });
        }
        if (paramEntityID != null) {
          record.setValue({
            fieldId: 'custrecord_lmry_co_rg_entity',
            value: entity_name
          });
        }
        //Creado Por
        record.setValue({
          fieldId: 'custrecord_lmry_co_rg_employee',
          value: usuario
        });

        record.save();
        libreria.sendrptuser(namereport, 3, Final_NameFile);
        return idfile;
      }
    }

    function ValidarAcentos(s) {
      var AccChars = "ŠŽšžŸÀÁÂÃÄÅÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖÙÚÛÜÝàáâãäåçèéêëìíîïðñòóôõöùúûüýÿ°–—ªº·&_!¡";
      var RegChars = "SZszYAAAAAACEEEEIIIIDNOOOOOUUUUYaaaaaaceeeeiiiidnooooouuuuyyo--ao.y   ";
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

    function ObtenerEntidad(paramEntity) {
      if (paramEntity != null && paramEntity != '') {
        var entity_customer_temp = search.lookupFields({
          type: search.Type.CUSTOMER,
          id: Number(paramEntity),
          columns: ['entityid', 'firstname', 'lastname', 'companyname', 'internalid', 'vatregnumber', 'custentity_lmry_digito_verificator']
        });

        var entity_id;

        entity_nit = entity_customer_temp.vatregnumber + entity_customer_temp.custentity_lmry_digito_verificator;

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
          entityCustomer = true;
          return true;
        } else {
          var entity_vendor_temp = search.lookupFields({
            type: search.Type.VENDOR,
            id: paramEntity,
            columns: ['entityid', 'firstname', 'lastname', 'companyname', 'internalid', 'vatregnumber', 'custentity_lmry_digito_verificator']
          });

          entity_nit = entity_vendor_temp.vatregnumber + entity_vendor_temp.custentity_lmry_digito_verificator;

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
            entityVendor = true;
            return true;
          } else {
            var entity_employee_temp = search.lookupFields({
              type: search.Type.EMPLOYEE,
              id: paramEntity,
              columns: ['entityid', 'firstname', 'lastname', 'internalid', 'custentity_lmry_sv_taxpayer_number', 'custentity_lmry_digito_verificator']
            });

            entity_nit = entity_employee_temp.custentity_lmry_sv_taxpayer_number + entity_employee_temp.custentity_lmry_digito_verificator;

            if (entity_employee_temp.internalid != null) {
              entity_id = (entity_employee_temp.internalid)[0].value;
            }

            entity_name = entity_employee_temp.firstname + ' ' + entity_employee_temp.lastname;

            if (entity_name == null && entity_name.trim() == '') {
              entity_name = entity_employee_temp.entityid;
            }

            if (entity_id != null) {
              entityEmployee = true;
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
