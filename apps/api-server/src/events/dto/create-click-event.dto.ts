import {
  IsUUID,
  IsString,
  IsOptional,
  IsISO8601,
  IsIn,
  ValidateNested,
  IsNotEmpty,
} from 'class-validator';
import { Type } from 'class-transformer';

export class ClickEventMetadataDto {
  @IsOptional()
  @IsString()
  referrer?: string;

  @IsOptional()
  @IsString()
  device?: string;

  @IsOptional()
  @IsString()
  ip?: string;
}

export class CreateClickEventDto {
  @IsUUID('4')
  eventId: string;

  @IsString()
  @IsNotEmpty()
  userId: string;

  @IsString()
  @IsNotEmpty()
  sessionId: string;

  @IsIn(['product_click', 'page_view'])
  eventType: 'product_click' | 'page_view';

  @IsOptional()
  @IsString()
  productId?: string;

  @IsString()
  @IsNotEmpty()
  pageUrl: string;

  @IsISO8601()
  occurredAt: string;

  @IsOptional()
  @ValidateNested()
  @Type(() => ClickEventMetadataDto)
  metadata?: ClickEventMetadataDto;
}