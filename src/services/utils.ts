import { RequestInfo } from 'node-fetch';

export function partitionArray<T>(array: readonly T[], partitionSize: number): readonly T[][] {
	const workingCopy: T[] = [...array];
	const result: T[][] = [];
	while (workingCopy.length) {
		result.push(workingCopy.splice(0, partitionSize));
	}
	return result;
}

export async function http(request: RequestInfo, options?: any): Promise<string> {
	const fetch = (await import('node-fetch')).default;
	return new Promise(resolve => {
		fetch(request, options)
			.then(
				response => {
					return response.text();
				},
				error => {
					console.warn('could not retrieve review', error);
				},
			)
			.then(body => {
				resolve(body as string);
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
		objectsByKeyValue[value] = objectsByKeyValue[value] ?? [];
		// Using push instead of concat is thousands of times faster on big arrays
		objectsByKeyValue[value].push(obj);
		return objectsByKeyValue;
	}, {});
};
