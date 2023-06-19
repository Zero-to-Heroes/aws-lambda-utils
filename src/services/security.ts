import SecretsManager, { GetSecretValueRequest, GetSecretValueResponse } from 'aws-sdk/clients/secretsmanager';
import { JwtPayload, verify } from 'jsonwebtoken';
import fetch from 'node-fetch';

const secretsManager = new SecretsManager({ region: 'us-west-2' });

export const validateOwToken = async (token: string): Promise<TokenValidationResult> => {
	const response = await fetch(
		`https://accounts.overwolf.com/tokens/short-lived/users/profile?extensionId=lnknbakkpommmjjdnelmfbjjdbocfpnpbkijjnob`,
		{
			method: 'GET',
			headers: {
				'Authorization': `Bearer ${token}`,
			},
		},
	);
	const validationResult: TokenValidationResult = (await response.json()) as TokenValidationResult;
	return validationResult;
};

export const validateFirestoneToken = async (token: string): Promise<TokenValidationResult | null> => {
	if (!token?.length) {
		return null;
	}

	const secretRequest: GetSecretValueRequest = {
		SecretId: 'sso',
	};
	const secret: SecretInfo = await getSecret(secretRequest);
	const payload: JwtPayload = verify(token, secret.fsJwtTokenKey) as JwtPayload;
	// Check if token is expired
	if (payload.exp * 1000 < Date.now()) {
		return null;
	}

	return {
		username: payload.userName,
	};
};

export interface TokenValidationResult {
	readonly username: string;
	readonly nickname?: string;
}

const getSecret = (secretRequest: GetSecretValueRequest) => {
	return new Promise<SecretInfo>(resolve => {
		secretsManager.getSecretValue(secretRequest, (err, data: GetSecretValueResponse) => {
			const secretInfo: SecretInfo = JSON.parse(data.SecretString);
			resolve(secretInfo);
		});
	});
};

interface SecretInfo {
	readonly clientId: string;
	readonly clientSecret: string;
	readonly fsJwtTokenKey: string;
}
