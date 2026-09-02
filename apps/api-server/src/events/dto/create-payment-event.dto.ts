import {
  IsUUID,
  IsString,
  IsNumber,
  Min,
  IsIn,
  IsISO8601,
  IsNotEmpty,
} from 'class-validator';

export class CreatePaymentEventDto {
  @IsUUID('4')
  eventId: string;

  @IsString()
  @IsNotEmpty()
  orderId: string;

  @IsString()
  @IsNotEmpty()
  userId: string;

  @IsNumber()
  @Min(0)
  amount: number;

  @IsString()
  @IsNotEmpty()
  currency: string;

  @IsString()
  @IsNotEmpty()
  paymentMethod: string;

  @IsIn(['completed', 'failed', 'canceled'])
  status: 'completed' | 'failed' | 'canceled';

  @IsISO8601()
  occurredAt: string;
}