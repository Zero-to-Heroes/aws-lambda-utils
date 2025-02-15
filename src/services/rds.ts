/* eslint-disable @typescript-eslint/no-use-before-define */
/* eslint-disable @typescript-eslint/no-var-requires */
import {
	GetSecretValueCommand,
	GetSecretValueCommandOutput,
	GetSecretValueRequest,
	SecretsManagerClient,
} from '@aws-sdk/client-secrets-manager';
import { createPool, MysqlError, PoolConnection } from 'mysql';
import { default as MySQLServerless, ServerlessMysql, default as serverlessMysql } from 'serverless-mysql';

const secretsManager = new SecretsManagerClient({ region: 'us-west-2' });
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

export const getConnectionProxy = async (): Promise<serverlessMysql.ServerlessMysql> => {
	const connect = async (): Promise<serverlessMysql.ServerlessMysql> => {
		const secretRequest: GetSecretValueRequest = {
			SecretId: 'rds-proxy',
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

export const streamQuery = async <T>(
	queryStr: string,
	onRow: (row: T, connection: PoolConnection) => Promise<void>,
	onEnd: () => Promise<void>,
): Promise<void> => {
	const secretRequest: GetSecretValueRequest = {
		SecretId: 'rds-proxy',
	};
	const secret: SecretInfo = await getSecret(secretRequest);
	const pool = createPool({
		connectionLimit: 1,
		host: secret.host,
		user: secret.username,
		password: secret.password,
		database: 'replay_summary',
		port: secret.port,
	});
	return new Promise<void>(resolve => {
		pool.getConnection(async (err: MysqlError, connection: PoolConnection) => {
			if (err) {
				console.log('error with connection', err);
				throw new Error('Could not connect to DB');
			}
			const query = connection.query(queryStr);
			query
				.on('error', err => {
					console.error('error while fetching rows', err);
				})
				// .on('fields', (fields) => {
				// 	console.log('fields', fields);
				// })
				.on('result', async (row: T) => {
					await onRow(row, connection);
				})
				.on('end', async () => {
					await onEnd();
					connection.release();
					pool.end(err => {
						console.log('ending pool', err);
					});
					resolve();
				});
		});
	});
};

const getSecret = async (secretRequest: GetSecretValueRequest): Promise<SecretInfo> => {
	try {
		const command = new GetSecretValueCommand(secretRequest);
		const data: GetSecretValueCommandOutput = await secretsManager.send(command);
		const secretInfo: SecretInfo = JSON.parse(data.SecretString || '{}');
		return secretInfo;
	} catch (err) {
		console.error('could not get secret value', err);
		return null;
	}
};

interface SecretInfo {
	readonly username: string;
	readonly password: string;
	readonly host: string;
	readonly hostReadOnly: string;
	readonly port: number;
	readonly dbClusterIdentifier: string;
}
