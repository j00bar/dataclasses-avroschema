from typing import Any, Callable, Dict, Optional, Type, TypeVar

from fastavro.validation import validate

from dataclasses_avroschema import serialization
from dataclasses_avroschema.schema_generator import AVRO, AvroModel
from dataclasses_avroschema.types import JsonDict
from dataclasses_avroschema.utils import standardize_custom_type

from .parser import PydanticV1Parser

try:
    from pydantic.v1 import BaseModel  # pragma: no cover
except ImportError as ex:  # pragma: no cover
    raise Exception("pydantic must be installed in order to use AvroBaseModel") from ex  # pragma: no cover

CT = TypeVar("CT", bound="AvroBaseModel")


class AvroBaseModel(BaseModel, AvroModel):  # type: ignore
    @classmethod
    def generate_dataclass(cls: Type[CT]) -> Type[CT]:
        return cls

    @classmethod
    def json_schema(cls: Type[CT], *args: Any, **kwargs: Any) -> str:
        return cls.schema_json(*args, **kwargs)

    def _standardize_type(self) -> Dict[str, Any]:
        """
        Standardization factory that converts data according to the
        user-defined pydantic json_encoders prior to passing values
        to the standard type conversion factory
        """
        encoders = self.__config__.json_encoders
        data = dict(self)

        for k, v in data.items():
            v_type = type(v)
            if v_type in encoders:
                encode_method = encoders[v_type]
                data[k] = encode_method(v)
        return data

    def asdict(self, standardize_factory: Optional[Callable[..., Any]] = None) -> JsonDict:
        """
        Returns this model in dictionary form. This method differs from
        pydantic's dict by converting all values to their Avro representation.
        It also doesn't provide the exclude, include, by_alias, etc.
        parameters that dict provides.
        """
        standardize_method = standardize_factory or standardize_custom_type
        
        return {
            field_name: standardize_method(
                field_name=field_name, value=value, model=self, base_class=AvroBaseModel
            )
            for field_name, value in self._standardize_type().items()
        }

    def serialize(self, serialization_type: str = AVRO) -> bytes:
        """
        Overrides the base AvroModel's serialize method to inject this
        class's standardization factory method
        """
        schema = self.avro_schema_to_python()

        return serialization.serialize(
            self.asdict(),
            schema,
            serialization_type=serialization_type,
        )

    def validate_avro(self) -> bool:
        """
        Validate that instance matches the avro schema
        """
        schema = self.avro_schema_to_python()
        return validate(self.asdict(), schema)

    @classmethod
    def fake(cls: Type[CT], **data: Any) -> CT:
        """
        Creates a fake instance of the model.

        Attributes:
            data: Dict[str, Any] represent the user values to use in the instance
        """
        # only generate fakes for fields that were not provided in data
        payload = {field.name: field.fake() for field in cls.get_fields() if field.name not in data.keys()}
        payload.update(data)

        return cls.parse_obj(payload)

    @classmethod
    def _generate_parser(cls: Type[CT]) -> PydanticV1Parser:
        cls._metadata = cls.generate_metadata()
        return PydanticV1Parser(type=cls._klass, metadata=cls._metadata, parent=cls._parent or cls)
