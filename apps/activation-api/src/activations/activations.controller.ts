import { Body, Controller, Get, HttpCode, Param, Post } from "@nestjs/common";
import { ActivationsService } from "./activations.service";
import { CreateActivationDto } from "./dto/create-activation.dto";

@Controller("activations")
export class ActivationsController {
  constructor(private readonly service: ActivationsService) {}

  /** RF-02: responde 202 Accepted con el activationId y estado PENDING, sin esperar a los demás servicios. */
  @Post()
  @HttpCode(202)
  create(@Body() dto: CreateActivationDto) {
    return this.service.create(dto);
  }

  /** RF-09: estado actual e historial de eventos. */
  @Get(":id")
  findOne(@Param("id") id: string) {
    return this.service.findOne(id);
  }
}
