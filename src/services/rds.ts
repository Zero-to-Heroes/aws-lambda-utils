/* eslint-disable @typescript-eslint/no-use-before-define */
/* eslint-disable @typescript-eslint/no-var-requires */
import { SecretsManager } from 'aws-sdk';
import { GetSecretValueRequest, GetSecretValueResponse } from 'aws-sdk/clients/secretsmanager';
import { default as MySQLServerless, ServerlessMysql, default as serverlessMysql } from 'serverless-mysql';

const secretsManager = new SecretsManager({ region: 'us-west-2' });
let connection, connectionPromise;

export const getConnection = async (): Promise<serverlessMysql.ServerlessMysql> => {
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

	if (connection) {
		return connection;
	}
	if (connectionPromise) {
		return connectionPromise;
	}
	connectionPromise = connect();

	return connectionPromise;
};

export const getConnectionReadOnly = async (): Promise<serverlessMysql.ServerlessMysql> => {
	const connect = async (): Promise<serverlessMysql.ServerlessMysql> => {
		const secretRequest: GetSecretValueRequest = {
			SecretId: 'rds-connection',
		};
		const secret: SecretInfo = await getSecret(secretRequest);
		const config = {
			host: secret.hostReadOnly,
			user: secret.username,
			password: secret.password,
			database: 'replay_summary',
			port: secret.port,
		};
		connection = MySQLServerless({ config });

		return connection;
	};

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
		console.log('running query', query);
	}
	const result: any[] = await mysql.query(query);
	if (debug) {
		console.log('result', result);
	}
	return result;
};

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
	readonly hostReadOnly: string;
	readonly port: number;
	readonly dbClusterIdentifier: string;
}
