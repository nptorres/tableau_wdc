(function () {
  const myConnector = tableau.makeConnector();
  const dataSetEndpoint = 'https://api.namara.io/v0/data_sets';
  const geometryFormat = 'wkt';

  const dataTypeMap = {
    boolean: tableau.dataTypeEnum.bool,
    currency: tableau.dataTypeEnum.float,
    date: tableau.dataTypeEnum.date,
    datetime: tableau.dataTypeEnum.datetime,
    decimal: tableau.dataTypeEnum.float,
    geojson: tableau.dataTypeEnum.geometry,
    integer: tableau.dataTypeEnum.int,
    percent: tableau.dataTypeEnum.float,
    string: tableau.dataTypeEnum.string,
    text: tableau.dataTypeEnum.string,
    time: tableau.dataTypeEnum.datetime,
    object: tableau.dataTypeEnum.string,
  };

  const transformColumn = column => ({
    id: column.key.replace(/\W/g, ''),
    alias: column.title,
    description: column.description,
    dataType: dataTypeMap[column.type] || tableau.dataTypeEnum.string,
  });

  const getMeta = metas => (
    metas.reduce((acc, meta) => {
      if (meta.language === 'en') {
        return acc = Object.assign({}, meta);
      }

      return acc;
    }, {})
  );

  const getLatestVersion = versions => (
    versions
      .filter(v => v.language === 'en')
      .sort((a, b) => (
        parseInt(b.identifier.split('-')[1], 10) - parseInt(a.identifier.split('-')[1], 10)
      ))[0]
  );

  const getSchema = (dataSetId, organizationId, apiKey) => {
    if (dataSetId == null || apiKey == null) return null;

    return new Promise((resolve, reject) => {
      $.ajax({
        url: `${dataSetEndpoint}/${dataSetId}?organization_id=${organizationId}`,
        headers: {
          'X-API-KEY': apiKey,
          'Content-Type': 'application/json',
        },
      })
        .done((res) => {
          resolve({
            title: getMeta(res.data_set_metas).title,
            version: getLatestVersion(res.versions),
          });
        })
        .fail(() => {
          throw new Error('Could not fetch namara data set schema...');
          reject();
        });
    });
  };

  myConnector.getSchema = (schemaCallback) => {
    const input = JSON.parse(tableau.connectionData);

    getSchema(input.dataSetId, input.organizationId, input.apiKey)
      .then((schema) => {
        input.dataSetVersion = schema.version.identifier;
        tableau.connectionData = JSON.stringify(input);

        const table = {
          id: schema.title.replace(/\W/g, ''),
          alias: schema.title,
          columns: schema.version.properties.map(transformColumn),
        };

        schemaCallback([table]);
      });
  };

  myConnector.getData = (table, doneCallback) => {
    const input = JSON.parse(tableau.connectionData);

    $.ajax({
      url: `${dataSetEndpoint}/${input.dataSetId}/data/${input.dataSetVersion}?geometry_format=${geometryFormat}&organization_id=${input.organizationId}`,
      headers: {
        'X-API-KEY': input.apiKey,
        'Content-Type': 'application/json',
      },
    })
      .done((data) => {
        const rows = JSON.parse(data || []);

        table.appendRows(rows);

        // TODO: get more than the first 250 rows
        doneCallback();
      })
      .fail(() => {
        throw new Error('Could not fetch namara data set rows...');
        reject();
      });
  };

  tableau.registerConnector(myConnector);

  $(document).ready(() => {
    $namaraForm = $("#namaraForm").on('submit', () => {
      const data = {};
      data.dataSetId = $('#dataSetId').val().trim();
      data.organizationId = $('#organizationId').val().trim();
      data.apiKey = $('#apiKey').val().trim();

      tableau.connectionData = JSON.stringify(data);
      tableau.connectionName = 'Namara Feed';
      tableau.submit();
    });
  });
})();
