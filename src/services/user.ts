import { ServerlessMysql } from 'serverless-mysql';
import SqlString from 'sqlstring';

export const getAllUserIds = async (
	mysql: ServerlessMysql,
	inputUserId: string,
	userName?: string,
): Promise<readonly string[]> => {
	const escape = SqlString.escape;
	const userSelectQuery = `
			SELECT DISTINCT userId FROM user_mapping
			INNER JOIN (
				SELECT DISTINCT username FROM user_mapping
				WHERE 
					(username = ${escape(userName)} OR username = ${escape(inputUserId)} OR userId = ${escape(inputUserId)})
					AND username IS NOT NULL
					AND username != ''
					AND username != 'null'
					AND userId != ''
					AND userId IS NOT NULL
					AND userId != 'null'
			) AS x ON x.username = user_mapping.username
			UNION ALL SELECT ${escape(inputUserId)}
		`;
	console.debug('running query', userSelectQuery);
	const userIds: any[] = await mysql.query(userSelectQuery);
	console.debug('got user result', userIds.length);
	return userIds.map(result => result.userId);
};
