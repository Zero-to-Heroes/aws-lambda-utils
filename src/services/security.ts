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
	const validationResult: TokenValidationResult = await response.json();
	return validationResult;
};

export interface TokenValidationResult {
	readonly username: string;
	readonly nickname: string;
}
