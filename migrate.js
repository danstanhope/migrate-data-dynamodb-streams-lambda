'use strict';

const pkg = require('./package.json');
const program = require('commander');
const DynamoDbAdapter = require('./DynamoDbAdapter');

const db = new DynamoDbAdapter();

let getData = async (params) => {
    return await db.scan(params);
}

let batchUpdateItems = async (items, table) => {
    let { Items, Count } = items;
    let index = 0;
    let batchUpdate = [];

    for (let item of Items) {
        index++;

        item.pre_existing_processed = "yes";

        batchUpdate.push({
            PutRequest: {
                Item: item
            }
        });

        if (index % 25 === 0 || index == Items.length) {
            let params = {
                RequestItems: {
                    [table]: batchUpdate
                }
            };

            await db.batchWrite(params);

            batchUpdate = [];
        }

    }
}

(async () => {

    program.version(pkg.version)
        .option('-t, --table-name <type>', 'table name')
        .option('-b, --batch-size <type>', 'batch size')
        .parse(process.argv);

    const options = program.opts();

    if (!options.tableName) {
        console.log('Whoa! Not so fast. Need a table name from ya?');

        program.outputHelp();

        process.exit(1);
    }

    try {
        let documents = [];
        let params = {
            TableName: options.tableName,
            FilterExpression: "attribute_not_exists(#s)",
            ExpressionAttributeNames: { "#s": "pre_existing_processed" }
        };
        let resp = await getData(params);

        await batchUpdateItems(resp, options.tableName);

        let batch = 1;

        while (resp.LastEvaluatedKey) {
            params.ExclusiveStartKey = resp.LastEvaluatedKey;

            resp = await getData(params);

            await batchUpdateItems(resp, options.tableName);

            if (options.batchSize && batch == options.batchSize){
                break;
            }

            batch++;
        }
    } catch (err) {
        console.log(err);
    }
})();
