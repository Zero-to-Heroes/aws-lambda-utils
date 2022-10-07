/* eslint-disable @typescript-eslint/no-use-before-define */
/* eslint-disable @typescript-eslint/no-var-requires */
import { SecretsManager } from 'aws-sdk';
import { GetSecretValueRequest, GetSecretValueResponse } from 'aws-sdk/clients/secretsmanager';
import { default as MySQLServerless, default as serverlessMysql, ServerlessMysql } from 'serverless-mysql';
import { logger } from './logger';

const secretsManager = new SecretsManager({ region: 'us-west-2' });
let connection, connectionPromise;

const connect = async (): Promise<serverlessMysql.ServerlessMysql> => {
	const secretRequest: GetSecretValueRequest = {
		SecretId: 'rds-connection',
	};
	const secret: SecretInfo = await getSecret(secretRequest);
	const config = {
		host: secret.host,
		user: secret.username,
		password: secret.password,
		database: 'replay_summary',
		port: secret.port,
	};
	connection = MySQLServerless({ config });

	return connection;
};

const getConnection = async (): Promise<serverlessMysql.ServerlessMysql> => {
	if (connection) {
		return connection;
	}
	if (connectionPromise) {
		return connectionPromise;
	}
	connectionPromise = connect();

	return connectionPromise;
};

export const runQuery = async (mysql: ServerlessMysql, query: string, debug = false): Promise<any[]> => {
	if (debug) {
		logger.log('running query', query);
	}
	const result: any[] = await mysql.query(query);
	if (debug) {
		logger.log('result', result);
	}
	return result;
};

export { getConnection };

const getSecret = (secretRequest: GetSecretValueRequest) => {
	return new Promise<SecretInfo>(resolve => {
		secretsManager.getSecretValue(secretRequest, (err, data: GetSecretValueResponse) => {
			const secretInfo: SecretInfo = JSON.parse(data.SecretString);
			resolve(secretInfo);
		});
	});
};

interface SecretInfo {
	readonly username: string;
	readonly password: string;
	readonly host: string;
	readonly port: number;
	readonly dbClusterIdentifier: string;
}
