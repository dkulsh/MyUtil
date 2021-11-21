
@transaction.atomic
def update_person_parameters(self, person_id, **kwargs):

    Person = apps.get_model("params", "Person")
    PersonSteps = apps.get_model("params", "PersonSteps")
    PersonHeartRate = apps.get_model("params", "PersonHeartRate")
    PersonTemperature = apps.get_model("params", "PersonTemperature")
    TrainingSlot = apps.get_model("slot", "TrainingSlot")
    TrainerBooking = apps.get_model("params", "TrainerBooking")

    steps = kwargs.pop("steps")
    heart_rate = kwargs.pop("heart_rate")
    temperature = kwargs.pop("temperature", [])

    trainer_bookings = self.model.objects.filter(
        status__in=(self.model.STATUS_UPCOMING, self.model.COMPLETED,
                    self.model.MISSED),
        person__person_id=person_id,
    ).values_list("primary_key", flat=True)

    if not trainer_bookings.exists():
        return # Nothing to do

    person_queryset = Person.objects.filter(
        booking_id__in=trainer_bookings, person_id=person_id
    )

    person_queryset.update(**kwargs)

    centers = HealthCenter.objects\
        .filter(pk__in=person_queryset.values("trainer_bookings__centers__primary_key")[:1])

    mapping_queryset = TrainingSlot.objects\
        .filter(center__bookings=OuterRef("trainer_booking"))
        .values("training_slot")[:1]

    Person.objects.update(session=Subquery(mapping_queryset))

    person_subquery_qs = Person.objects
        .filter(trainer_booking=OuterRef("primary_key"))
        .values("training_slot")[:1]

    TrainerBooking.objects.filter(guests__in=person_queryset)\
        .update(session=Subquery(person_subquery_qs))

    #     A lot of other work
    #     ...
    #     ...
    #     ...
