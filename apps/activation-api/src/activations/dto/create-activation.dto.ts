import { IsIn, IsNotEmpty, IsOptional, IsString } from "class-validator";

export class CreateActivationDto {
  @IsString()
  @IsNotEmpty()
  customerId: string;

  @IsString()
  @IsNotEmpty()
  planId: string;

  @IsOptional()
  @IsString()
  channel?: string;

  @IsOptional()
  @IsIn(["none", "billing", "provisioning"])
  simulateFailure?: "none" | "billing" | "provisioning";
}
