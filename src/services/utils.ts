import fetch, { RequestInfo } from 'node-fetch';
import { logger } from './logger';

export function partitionArray<T>(array: readonly T[], partitionSize: number): readonly T[][] {
	const workingCopy: T[] = [...array];
	const result: T[][] = [];
	while (workingCopy.length) {
		result.push(workingCopy.splice(0, partitionSize));
	}
	return result;
}

export async function http(request: RequestInfo): Promise<string> {
	return new Promise(resolve => {
		fetch(request)
			.then(
				response => {
					return response.text();
				},
				error => {
					logger.warn('could not retrieve review', error);
				},
			)
			.then(body => {
				resolve(body);
			});
	});
}

export async function sleep(ms) {
	return new Promise(resolve => setTimeout(resolve, ms));
}

export const encode = (input: string): string => {
	const buff = Buffer.from(input, 'utf-8');
	const base64 = buff.toString('base64');
	return base64;
};

export const decode = (base64: string): string => {
	const buff = Buffer.from(base64, 'base64');
	const str = buff.toString('utf-8');
	return str;
};

export const groupByFunction = <T>(keyExtractor: (obj: T) => string | number) => (
	array: readonly T[],
): { [key: string]: readonly T[] } => {
	return (array ?? []).reduce((objectsByKeyValue, obj) => {
		const value = keyExtractor(obj);
		objectsByKeyValue[value] = (objectsByKeyValue[value] || []).concat(obj);
		return objectsByKeyValue;
	}, {});
};
