'use strict';

const aws = require('aws-sdk');

class DynamoDbAdapter {
    constructor() {
        aws.config.loadFromPath('./aws_keys.json');
        this.dynamodb = new aws.DynamoDB.DocumentClient();
    }

    async scan(params) {
        return new Promise((resolve, reject) => {
            this.dynamodb.scan(params, (error, response) => {
                if (error) {
                    reject(new Error(error));
                } else {
                    resolve(response);
                }
            });
        });
    }

    async batchWrite(params) {
        return new Promise((resolve, reject) => {
            this.dynamodb.batchWrite(params, (error, response) => {
                if (error) {
                    reject(new Error(error));
                } else {
                    resolve();
                }
            });
        });
    }
}

module.exports = DynamoDbAdapter;