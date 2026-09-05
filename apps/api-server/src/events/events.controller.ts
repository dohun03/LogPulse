import { Body, Controller, HttpCode, HttpStatus, Post, UseGuards } from '@nestjs/common';
import { EventsService } from './events.service';
import { CreateClickEventDto } from './dto/create-click-event.dto';
import { CreatePaymentEventDto } from './dto/create-payment-event.dto';
import { ApiKeyGuard } from '../common/guards/api-key.guard';

@UseGuards(ApiKeyGuard)
@Controller('events')
export class EventsController {
  constructor(
    private readonly eventsService: EventsService,
  ) {}

  @Post('click')
  @HttpCode(HttpStatus.ACCEPTED)
  createClickEvent(@Body() dto: CreateClickEventDto) {
    return this.eventsService.publishClickEvent(dto);
  }

  @Post('payment')
  @HttpCode(HttpStatus.ACCEPTED)
  createPaymentEvent(@Body() dto: CreatePaymentEventDto) {
    return this.eventsService.publishPaymentEvent(dto);
  }
}